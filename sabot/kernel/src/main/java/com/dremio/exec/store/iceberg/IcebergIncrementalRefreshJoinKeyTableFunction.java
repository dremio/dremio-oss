/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.planner.physical.TableFunctionUtil.getDataset;
import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;
import static com.dremio.exec.store.iceberg.IcebergUtils.createFileIOForIcebergMetadata;
import static com.dremio.exec.store.iceberg.IcebergUtils.getColumnName;
import static com.dremio.exec.store.iceberg.IcebergUtils.loadTableMetadata;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.extractArgFromOneArgTransform;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.isTransformTypeSupportedBy;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.safeGetTransformType;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.toDisplayString;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;
import static com.dremio.service.namespace.dataset.proto.PartitionProtobuf.IcebergTransformType;
import static com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import static com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.IncrementalRefreshJoinKeyTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * A table function that raises the data found in the partition info to the required
 * outputPartitionSpec level If the partition info does not contain partition on the column
 * specified, or it cannot be raised to outputPartitionSpec level, UNSUPPORTED_PARTITION_EVOLUTION
 * exception is thrown Example1: current partition info is day(col1), target is month(col1) we can
 * raise successfully. Example2: current partition info is year(col1), target is month(col1), we
 * cannot calculate month(col1) so we fail with UNSUPPORTED_PARTITION_EVOLUTION Example3: current
 * partition info is day(col1), target is month(col2), we cannot calculate month(col2) so we fail
 * with UNSUPPORTED_PARTITION_EVOLUTION
 */
public class IcebergIncrementalRefreshJoinKeyTableFunction extends AbstractTableFunction {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(IcebergIncrementalRefreshJoinKeyTableFunction.class);

  public static final String UNSUPPORTED_PARTITION_EVOLUTION = "Unsupported Partition Evolution.";

  public static final String DREMIO_NULL = "D_R_E_M_I_O_N_U_L_L";
  public static final String DREMIO_BYTE_STRING = "D_R_E_M_I_O_B_Y_T_E_S_T_R";

  private final List<TransferPair> transfers = new ArrayList<>();
  private final PartitionSpec outputPartitionSpec;
  private boolean doneWithRow = false;
  private int inputIndex;
  private VarBinaryVector inputPartitionInfoVec;
  private IntVector inputSpecId;
  private VarCharVector outputIncrementalRefreshJoinKey;
  private Map<Integer, PartitionSpec> partitionSpecMap;
  private final IncrementalRefreshJoinKeyTableFunctionContext functionContext;
  private final FragmentExecutionContext fragmentExecutionContext;
  private final OpProps props;

  public IcebergIncrementalRefreshJoinKeyTableFunction(
      final FragmentExecutionContext fragmentExecutionContext,
      final OperatorContext context,
      final OpProps props,
      final TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    functionContext =
        (IncrementalRefreshJoinKeyTableFunctionContext) functionConfig.getFunctionContext();
    outputPartitionSpec =
        IcebergSerDe.deserializePartitionSpec(
            deserializedJsonAsSchema(functionContext.getIcebergSchema()),
            functionContext.getPartitionSpec().toByteArray());
    this.fragmentExecutionContext = fragmentExecutionContext;
    this.props = props;
  }

  @Override
  public VectorAccessible setup(final VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    final SupportsIcebergMutablePlugin icebergMutablePlugin =
        fragmentExecutionContext.getStoragePlugin(
            functionConfig.getFunctionContext().getPluginId());
    final FileIO io =
        createFileIOForIcebergMetadata(
            icebergMutablePlugin,
            context,
            functionContext.getPluginId().getName(),
            props,
            getDataset(functionConfig),
            functionContext.getMetadataLocation());
    final TableMetadata tableMetadata =
        loadTableMetadata(io, context, functionContext.getMetadataLocation());
    partitionSpecMap = tableMetadata.specsById();
    inputSpecId = (IntVector) getVectorFromSchemaPath(incoming, SystemSchemas.PARTITION_SPEC_ID);
    inputPartitionInfoVec =
        (VarBinaryVector) getVectorFromSchemaPath(incoming, SystemSchemas.PARTITION_INFO);
    outputIncrementalRefreshJoinKey =
        (VarCharVector)
            getVectorFromSchemaPath(outgoing, SystemSchemas.INCREMENTAL_REFRESH_JOIN_KEY);

    for (final Field field : incoming.getSchema()) {
      final ValueVector vvIn = getVectorFromSchemaPath(incoming, field.getName());
      final ValueVector vvOut = getVectorFromSchemaPath(outgoing, field.getName());
      final TransferPair tp = vvIn.makeTransferPair(vvOut);
      transfers.add(tp);
    }
    return outgoing;
  }

  @Override
  public void startRow(final int row) throws Exception {
    doneWithRow = false;
    inputIndex = row;
  }

  @Override
  public void startBatch(final int records) {
    outgoing.allocateNew();
  }

  @Override
  public int processRow(final int startOutIndex, final int maxRecords) throws Exception {
    if (doneWithRow) {
      return 0;
    }

    final NormalizedPartitionInfo sourcePartitionInfo =
        IcebergSerDe.deserializeFromByteArray(inputPartitionInfoVec.get(inputIndex));
    final int specID = inputSpecId.get(inputIndex);
    final PartitionSpec sourcePartitionSpec = partitionSpecMap.get(specID);

    final StringBuilder stringBuilder = new StringBuilder();
    boolean isFirst = true;
    for (final PartitionField targetPartitionField : outputPartitionSpec.fields()) {
      final Object transformedValue =
          verifyPartitionSupportedAndTransform(
              targetPartitionField, outputPartitionSpec, sourcePartitionInfo, sourcePartitionSpec);
      if (isFirst) {
        isFirst = false;
      } else {
        stringBuilder.append('#');
      }
      stringBuilder.append(getTransformedValueAsString(transformedValue));
    }
    outputIncrementalRefreshJoinKey.setSafe(startOutIndex, stringBuilder.toString().getBytes());
    if (startOutIndex == incoming.getRecordCount() - 1) {
      transfers.forEach(TransferPair::transfer);
      outgoing.setAllCount(incoming.getRecordCount());
    }
    doneWithRow = true;
    return 1;
  }

  /**
   * Converts the passed in transformed value into a string suitable for incremental refresh join
   * key comparison In addition does special handling for null
   */
  private String getTransformedValueAsString(final Object transformedValue) {
    if (transformedValue == null) {
      return DREMIO_NULL;
    }
    if (transformedValue instanceof ByteString) {
      // this happens if we partition by identity of a decimal(38, 10) column
      // we treat the transformed value as a binary string
      // we don't want to use a particular encoding such as UTF8, as it might throw a
      // UnsupportedEncodingException
      // instead we Base64 encode the ByteString and use that for join key comparison
      return DREMIO_BYTE_STRING
          + Base64.getEncoder().encodeToString(((ByteString) transformedValue).toByteArray());
    }
    return transformedValue.toString();
  }

  public static class IncrementalRefreshTransformNotPossibleException extends Exception {
    public IncrementalRefreshTransformNotPossibleException() {
      super();
    }
  }

  /**
   * Given a sourcePartitionInfo apply the transformation stored in targetPartitionField If not
   * possible throw UNSUPPORTED_PARTITION_EVOLUTION exception
   *
   * @param targetPartitionField target partition to transform to
   * @param targetPartitionSpec target partition spec
   * @param sourcePartitionInfo input partition info for the current row
   * @param sourcePartitionSpec partition spec for the current row
   * @return the transformed value
   */
  protected static Object verifyPartitionSupportedAndTransform(
      final PartitionField targetPartitionField,
      final PartitionSpec targetPartitionSpec,
      final NormalizedPartitionInfo sourcePartitionInfo,
      final PartitionSpec sourcePartitionSpec) {
    final Optional<IcebergTransformType> targetTransformation =
        safeGetTransformType(targetPartitionField.transform().toString());
    final String targetName = getColumnName(targetPartitionField, targetPartitionSpec.schema());
    for (final PartitionProtobuf.PartitionValue sourcePartitionValue :
        sourcePartitionInfo.getIcebergValuesList()) {
      try {
        if (sourcePartitionValue.getColumn().equals(targetName)) {
          final Optional<PartitionField> sourcePartitionField =
              sourcePartitionSpec.fields().stream()
                  // remove void transforms from previous partition evolutions and
                  // remove transforms on columns other than the one we are looking for
                  .filter(
                      f ->
                          safeGetTransformType(f.transform().toString()).isPresent()
                              && getColumnName(f, sourcePartitionSpec.schema())
                                  .equalsIgnoreCase(targetName))
                  .findAny();
          if (!sourcePartitionField.isPresent()) {
            continue;
          }
          final Optional<IcebergTransformType> sourceTransformation =
              safeGetTransformType(sourcePartitionField.get().transform().toString());
          if (targetTransformation.isPresent()
              && sourceTransformation.isPresent()
              && isTransformTypeSupportedBy(
                  targetTransformation.get(),
                  sourceTransformation.get(),
                  extractArgFromOneArgTransform(targetPartitionSpec, targetPartitionField),
                  extractArgFromOneArgTransform(sourcePartitionSpec, sourcePartitionField.get()),
                  targetPartitionSpec.schema().findField(targetPartitionField.sourceId()).type())) {
            switch (targetTransformation.get()) {
              case IDENTITY:
                return getValue(sourcePartitionValue);
              case TRUNCATE:
                return applyTransformation(
                    targetPartitionField,
                    getValue(sourcePartitionValue),
                    targetPartitionSpec.schema().findType(targetPartitionField.sourceId()));
              case HOUR:
              case DAY:
              case MONTH:
              case YEAR:
                // it is one of the supported datetime transforms
                final Object reversedTransformValue =
                    reverseTransformation(
                        sourceTransformation.get(), getValue(sourcePartitionValue));
                final Transform transform =
                    getUnboundIcebergTransformFromType(targetTransformation.get());
                // we use Timestamp here, because reverseTransformation always converts to timestamp
                return transform
                    .bind(Types.TimestampType.withoutZone())
                    .apply(reversedTransformValue);
              default:
                // do nothing
            }
          }
        }
      } catch (final IncrementalRefreshTransformNotPossibleException e) {
        // ignore the exception and move to the next sourcePartitionValue
        // it is possible we find a match with the next sourcePartitionValue, so we don't fail here
        // for example target partition is Month. We have partition both on Year and Day.
        // We will fail with Year if we encounter it first, but eventually we will find a match with
        // Day
      }
    }
    // If we are here, we did not find a partition value that can be transformed
    // As we already verified the current partition spec is correct and compatible with
    // targetPartitionField,
    // this will usually happen when there are some data files still using an old partition spec
    // We need to fail the incremental refresh, otherwise the data could be wrong
    // Data correctness takes precedence over successful incremental refresh

    final String message =
        String.format(
            "%s Tried to apply transformation:%s. Found records with partition spec:%s",
            UNSUPPORTED_PARTITION_EVOLUTION,
            toDisplayString(targetPartitionField, targetPartitionSpec),
            toDisplayString(sourcePartitionSpec));
    logger.error(message);
    throw UserException.unsupportedError().message(message).buildSilently();
  }

  private static Object applyTransformation(
      final PartitionField targetPartitionField,
      final Object reversedTransformValue,
      final Type type) {
    if (reversedTransformValue == null) {
      // null transforms to null
      return null;
    }
    final Transform transform = targetPartitionField.transform();
    return transform.bind(type).apply(reversedTransformValue);
  }

  /**
   * Returns an equivalent unbound Iceberg transform for transformType
   *
   * @param transformType transform type for
   * @return an equivalent unbound Iceberg transform or null
   */
  private static Transform getUnboundIcebergTransformFromType(
      final IcebergTransformType transformType) {
    switch (transformType) {
      case YEAR:
        return Transforms.year();
      case MONTH:
        return Transforms.month();
      case DAY:
        return Transforms.day();
      case HOUR:
        return Transforms.hour();
      default:
        return null;
    }
  }

  /**
   * Attempt to revert a transformation. Not all transforms are reversible The revert will be
   * approximate beginning of interval, and not the exact input value. If not possible throw a
   * IncrementalRefreshTransformNotPossibleException
   *
   * @param sourceTransformation transformation to attempt to revert
   * @param value input transformed value
   * @return reverted value or null if unsuccessful
   * @throws IncrementalRefreshTransformNotPossibleException if unsuccessful
   */
  private static Object reverseTransformation(
      final IcebergTransformType sourceTransformation, final Object value)
      throws IncrementalRefreshTransformNotPossibleException {
    switch (sourceTransformation) {
      case IDENTITY:
      case TRUNCATE:
        // Truncate transform is not really reversible.
        // We have special handling to only allow truncate if it can be reapplied safely
        // in IncrementalRefreshByPartitionUtils.truncateTransformsCompatible
        return value;
      case HOUR:
        return reverseTimeTransform(value, TimeUnit.HOURS);
      case DAY:
        return reverseTimeTransform(value, TimeUnit.DAYS);
      case MONTH:
        return reverseMonthTransform(value);
      case YEAR:
        return reverseYearTransform(value);
      default:
        // BUCKET is not reversible and not supported for this feature
        return null;
    }
  }

  /**
   * Revert a month transform by returning a timestamp representing the beginning of the month
   *
   * @param input number of months since 1970 Jan 1st 0:00:00 am
   * @return microseconds representing the beginning of the month
   * @throws IncrementalRefreshTransformNotPossibleException if unsuccessful
   */
  protected static Object reverseMonthTransform(final Object input)
      throws IncrementalRefreshTransformNotPossibleException {
    if (input == null) {
      // null reverses to null
      return null;
    }
    if (input instanceof Long || input instanceof Integer) {
      final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.set(
          1970 + (((Number) input).intValue() / 12),
          (((Number) input).intValue() % 12),
          1,
          0,
          0,
          0);
      // calendar.getTime returns milliseconds, but Iceberg Uses microseconds,so add *1_000
      return calendar.getTime().getTime() * 1_000;
    }
    throw new IncrementalRefreshTransformNotPossibleException();
  }

  /**
   * Revert a year transform by returning a timestamp representing the beginning of the year
   *
   * @param input number of years since 1970 Jan 1st 0:00:00 am
   * @return microseconds representing the beginning of the year
   * @throws IncrementalRefreshTransformNotPossibleException if unsuccessful
   */
  private static Object reverseYearTransform(final Object input)
      throws IncrementalRefreshTransformNotPossibleException {
    if (input == null) {
      // null reverses to null
      return null;
    }
    if (input instanceof Long || input instanceof Integer) {
      final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.set(1970 + ((Number) input).intValue(), 1, 1, 0, 0, 0);
      // calendar.getTime returns milliseconds, but Iceberg Uses microseconds, so add *1_000
      return calendar.getTime().getTime() * 1_000;
    }

    throw new IncrementalRefreshTransformNotPossibleException();
  }

  /**
   * Revert a time transform by returning a timestamp representing the beginning of the time period
   *
   * @param input number of time periods since 1970 Jan 1st 0:00:00 am
   * @return microseconds representing the beginning of the time period
   * @throws IncrementalRefreshTransformNotPossibleException if unsuccessful
   */
  private static Object reverseTimeTransform(final Object input, final TimeUnit timeUnit)
      throws IncrementalRefreshTransformNotPossibleException {
    if (input == null) {
      // null reverses to null
      return null;
    }
    if (input instanceof Long) {
      return timeUnit.toMicros((long) input);
    }
    if (input instanceof Integer) {
      return timeUnit.toMicros((int) input);
    }
    throw new IncrementalRefreshTransformNotPossibleException();
  }

  /**
   * Extract a value from PartitionValue
   *
   * @param input PartitionValue to extract value from
   * @return Object representing the value
   */
  private static Object getValue(final PartitionValue input) {
    if (input.hasBitValue()) {
      return input.getBitValue();
    }
    if (input.hasIntValue()) {
      return input.getIntValue();
    }
    if (input.hasLongValue()) {
      return input.getLongValue();
    }
    if (input.hasFloatValue()) {
      return input.getFloatValue();
    }
    if (input.hasDoubleValue()) {
      return input.getDoubleValue();
    }
    if (input.hasBinaryValue()) {
      return input.getBinaryValue();
    }
    if (input.hasStringValue()) {
      return input.getStringValue();
    }
    return null;
  }

  @Override
  public void closeRow() throws Exception {}
}
