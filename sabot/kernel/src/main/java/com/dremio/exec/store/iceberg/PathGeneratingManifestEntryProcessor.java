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

import static com.dremio.exec.store.iceberg.IcebergPartitionData.getPartitionColumnClass;
import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;
import static com.dremio.exec.store.iceberg.IcebergUtils.getValueFromByteBuffer;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;
import static com.dremio.exec.store.iceberg.model.IcebergConstants.FILE_VERSION;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DremioManifestReaderUtils.ManifestEntryWrapper;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.vector.OptionalVarBinaryVectorHolder;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Manifest entry processor which handles both data and delete manifest scans.
 */
public class PathGeneratingManifestEntryProcessor implements ManifestEntryProcessor {

  private static final Set<String> BASE_OUTPUT_FIELDS =
    Stream.concat(
      Stream.concat(
        Stream.concat(
          SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.getFields().stream(),
          SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA.getFields().stream()),
          SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA.getFields().stream()) ,
          Stream.of(SystemSchemas.ICEBERG_METADATA_FIELD))
      .map(f -> f.getName().toLowerCase())
      .collect(Collectors.toSet());

  private final BatchSchema outputSchema;
  private final Map<String, Field> nameToFieldMap;
  private final Set<String> invalidColumnsForPruning;
  private final Map<Field, ValueVector> columnStatsVectorMap = new HashMap<>();
  private final ManifestContent manifestContent;

  private byte[] colIdMapRaw;
  private Map<String, Integer> colToIDMap;
  private VarBinaryVector inputColIds;
  private VarCharVector outputFilePath;
  private BigIntVector outputFileSize;
  private BigIntVector outputSequenceNumber;
  private IntVector outputSpecId;
  private VarBinaryVector outputPartitionKey;
  private VarBinaryVector outputPartitionInfo;
  private VarBinaryVector outputColIds;
  private StructVector outputDeleteFile;
  private VarCharVector outputFileContent;
  private PartitionSpec icebergPartitionSpec;
  private Map<String, Integer> partColToKeyMap;
  private ArrowBuf tempBuf;

  // Optionals apply only if the field is part of schema
  private OptionalVarBinaryVectorHolder outputIcebergMetadata;

  private boolean doneWithCurrentEntry;

  public PathGeneratingManifestEntryProcessor(OperatorContext context,
      ManifestScanTableFunctionContext functionContext) {
    outputSchema = functionContext.getFullSchema();
    BatchSchema tableSchema = functionContext.getTableSchema();
    nameToFieldMap = tableSchema.getFields().stream().collect(Collectors.toMap(f -> f.getName().toLowerCase(), f -> f));
    Map<Integer, PartitionSpec> partitionSpecMap = functionContext.getJsonPartitionSpecMap() != null ?
        IcebergSerDe.deserializeJsonPartitionSpecMap(
            deserializedJsonAsSchema(functionContext.getIcebergSchema()),
            functionContext.getJsonPartitionSpecMap().toByteArray()) :
        null;
    invalidColumnsForPruning = IcebergUtils.getInvalidColumnsForPruning(partitionSpecMap);
    manifestContent = functionContext.getManifestContent();
    tempBuf = context.getManagedBuffer();
  }

  @Override
  public void setup(VectorAccessible incoming, VectorAccessible outgoing) {
    inputColIds = (VarBinaryVector) getVectorFromSchemaPath(incoming, SystemSchemas.COL_IDS);
    outputSequenceNumber = (BigIntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.SEQUENCE_NUMBER);
    outputSpecId = (IntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.PARTITION_SPEC_ID);
    outputPartitionKey = (VarBinaryVector) getVectorFromSchemaPath(outgoing, SystemSchemas.PARTITION_KEY);
    outputFilePath = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.DATAFILE_PATH);
    outputFileSize = (BigIntVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_SIZE);
    outputPartitionInfo = (VarBinaryVector) getVectorFromSchemaPath(outgoing, SystemSchemas.PARTITION_INFO);
    outputColIds = (VarBinaryVector) getVectorFromSchemaPath(outgoing, SystemSchemas.COL_IDS);
    outputFileContent = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_CONTENT);
    // output columns vary between data and delete manifest scans
    if (manifestContent == ManifestContent.DELETES) {
      outputDeleteFile = (StructVector) getVectorFromSchemaPath(outgoing, SystemSchemas.DELETE_FILE);
    }

    outputIcebergMetadata = new OptionalVarBinaryVectorHolder(outgoing, SystemSchemas.ICEBERG_METADATA);

    for (Field field : outputSchema.getFields()) {
      if (isColumnStatsOutputField(field)) {
        ValueVector vector = outgoing.getValueAccessorById(TypeHelper.getValueVectorClass(field),
            outgoing.getSchema().getFieldId(SchemaPath.getSimplePath(field.getName())).getFieldIds()).getValueVector();
        columnStatsVectorMap.put(field, vector);
      }
    }
  }

  @Override
  public void initialise(PartitionSpec partitionSpec, int row) {
    icebergPartitionSpec = partitionSpec;
    setColIdMap(row);
    partColToKeyMap = new HashMap<>();

    for (int i = 0; i < icebergPartitionSpec.fields().size(); i++) {
      PartitionField partitionField = icebergPartitionSpec.fields().get(i);
      if (partitionField.transform().isIdentity()) {
        partColToKeyMap.put(icebergPartitionSpec.schema().findField(partitionField.sourceId()).name().toLowerCase(), i);
      }
    }
  }

  private void setColIdMap(int row) {
    if (colToIDMap == null || colIdMapRaw == null) {
      colToIDMap = getColToIDMap(row);
      colIdMapRaw = inputColIds.get(row);
    }
  }

  @Override
  public int processManifestEntry(ManifestEntryWrapper<?> manifestEntry, int startOutIndex,
      int maxOutputCount) throws IOException {
    if (!shouldProcessCurrentEntry(maxOutputCount)) {
      return 0;
    }
    String modifiedPath = manifestEntry.file().path().toString();
    byte[] path = modifiedPath.getBytes();
    outputIcebergMetadata.setSafe(startOutIndex, () -> getIcebergMetadata(manifestEntry));
    outputSequenceNumber.setSafe(startOutIndex, manifestEntry.sequenceNumber());
    outputSpecId.setSafe(startOutIndex, manifestEntry.file().specId());
    outputPartitionKey.setSafe(startOutIndex, serializePartitionKey(manifestEntry.file().partition()));
    outputFilePath.setSafe(startOutIndex, path);
    outputFileSize.setSafe(startOutIndex, manifestEntry.file().fileSizeInBytes());
    outputColIds.setSafe(startOutIndex, colIdMapRaw);
    IcebergFileType fileType = IcebergFileType.valueById(manifestEntry.file().content().id());
    outputFileContent.setSafe(startOutIndex, fileType.name().getBytes(StandardCharsets.UTF_8));

    long version = PathUtils.getQueryParam(manifestEntry.file().path().toString(), FILE_VERSION, 0L, Long::parseLong);
    Schema fileSchema = icebergPartitionSpec.schema();
    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = ManifestEntryProcessorHelper.getDataFilePartitionInfo(
      icebergPartitionSpec,
      invalidColumnsForPruning,
      fileSchema,
      nameToFieldMap,
      manifestEntry.file(),
      version,
      manifestEntry.sequenceNumber());
    outputPartitionInfo.setSafe(startOutIndex, IcebergSerDe.serializeToByteArray(partitionInfo));

    if (manifestContent == ManifestContent.DELETES) {
      NullableStructWriter structWriter = outputDeleteFile.getWriter();
      structWriter.setPosition(startOutIndex);
      structWriter.start();
      tempBuf = tempBuf.reallocIfNeeded(path.length);
      tempBuf.setBytes(0, path);
      structWriter.varChar(SystemSchemas.PATH).writeVarChar(0, path.length, tempBuf);
      structWriter.integer(SystemSchemas.FILE_CONTENT).writeInt(manifestEntry.file().content().id());
      structWriter.bigInt(SystemSchemas.RECORD_COUNT).writeBigInt(manifestEntry.file().recordCount());
      BaseWriter.ListWriter listWriter = structWriter.list(SystemSchemas.EQUALITY_IDS);
      if (manifestEntry.file().content() == FileContent.EQUALITY_DELETES) {
        List<Integer> equalityIds = manifestEntry.file().equalityFieldIds();
        Preconditions.checkState(equalityIds != null && equalityIds.size() > 0,
            "Equality delete file %s missing required equality_ids field", manifestEntry.file().path());
        listWriter.startList();
        for (Integer equalityId : equalityIds) {
          listWriter.integer().writeInt(equalityId);
        }
        listWriter.endList();
      } else {
        listWriter.writeNull();
      }
      structWriter.end();
    }

    Map<String, Object> columnStats = getColumnStats(manifestEntry.file(), version);
    for (Field field : outputSchema.getFields()) {
      if (isColumnStatsOutputField(field)) {
        ValueVector vector = columnStatsVectorMap.get(field);
        writeToVector(vector, startOutIndex, columnStats.get(field.getName()));
      }
    }

    doneWithCurrentEntry = true;
    return 1;
  }

  private byte[] getIcebergMetadata(ManifestEntryWrapper manifestEntry) {
    try {
      IcebergMetadataInformation metadataInformation = new IcebergMetadataInformation(
        IcebergSerDe.serializeToByteArray(manifestEntry.file()));
      return IcebergSerDe.serializeToByteArray(metadataInformation);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void closeManifestEntry() {
    doneWithCurrentEntry = false;
  }

  @Override
  public void close() throws Exception {
    // Nothing to close
  }

  private boolean shouldProcessCurrentEntry(int maxOutputCount) {
    return !doneWithCurrentEntry && maxOutputCount > 0;
  }

  private Map<String, Integer> getColToIDMap(int row) {
    if (colToIDMap == null) {
      Preconditions.checkArgument(inputColIds.getValueCount() > 0);
      IcebergProtobuf.IcebergDatasetXAttr icebergDatasetXAttr;
      try {
        icebergDatasetXAttr = LegacyProtobufSerializer.parseFrom(
          IcebergProtobuf.IcebergDatasetXAttr.PARSER, inputColIds.get(row));
      } catch (InvalidProtocolBufferException ie) {
        throw new RuntimeException("Could not deserialize Iceberg dataset info", ie);
      } catch (Exception e) {
        throw new RuntimeException("Unable to get colIDMap");
      }
      return icebergDatasetXAttr.getColumnIdsList().stream().collect(Collectors.toMap(c -> c.getSchemaPath().toLowerCase(), c -> c.getId()));
    } else {
      return colToIDMap;
    }
  }

  private Map<String, Object> getColumnStats(ContentFile<? extends ContentFile<?>> currentFile, long version) {
    Map<String, Object> requiredStats = new HashMap<>();

    Schema fileSchema = icebergPartitionSpec.schema();
    for (Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (!isColumnStatsOutputField(field)) {
        continue;
      }
      Preconditions.checkArgument(fieldName.length() > 4);
      String colName = fieldName.substring(0, fieldName.length() - 4).toLowerCase();
      String suffix = fieldName.substring(fieldName.length() - 3).toLowerCase();

      if (colName.equals(IncrementalUpdateUtils.UPDATE_COLUMN)) {
        requiredStats.put(fieldName, version);
        continue;
      }

      Preconditions.checkArgument(colToIDMap.containsKey(colName));
      int key = colToIDMap.get(colName);
      Types.NestedField icebergField = fileSchema.findField(key);
      if (icebergField == null) {
        requiredStats.put(fieldName, null);
        continue;
      }
      Type fieldType = icebergField.type();

      Object value = null;

      switch (suffix) {
        case "min":
          ByteBuffer lowerBound = null;
          if (currentFile.lowerBounds() != null) {
            lowerBound = currentFile.lowerBounds().get(key);
          }
          value = getValueFromByteBuffer(lowerBound, fieldType);
          break;
        case "max":
          ByteBuffer upperBound = null;
          if (currentFile.upperBounds() != null) {
            upperBound = currentFile.upperBounds().get(key);
          }
          value = getValueFromByteBuffer(upperBound, fieldType);
          break;
        case "val":
          //For select, there will never be a case
          // where partColToKeyMap doesn't have a colName.
          // It always brings those data files which have identity partition details.
          //In case of OPTIMIZE,
          // it can fetch the data files which have non-identity partitions also where partition evolution has happened.
          if (partColToKeyMap.containsKey(colName)) {
            int partColPos = partColToKeyMap.get(colName);
            value = currentFile.partition().get(partColPos, getPartitionColumnClass(icebergPartitionSpec, partColPos));
          }
          break;
        default:
          throw new RuntimeException("unexpected suffix for column: " + fieldName);
      }
      requiredStats.put(fieldName, value);
    }

    return requiredStats;
  }

  private boolean isColumnStatsOutputField(Field field) {
    return !BASE_OUTPUT_FIELDS.contains(field.getName().toLowerCase());
  }

  private byte[] serializePartitionKey(StructLike partition) throws IOException {
    Object[] vals = new Object[partition.size()];
    for (int i = 0; i < partition.size(); i++) {
      vals[i] = partition.get(i, Object.class);
    }

    return IcebergSerDe.serializeToByteArray(vals);
  }
}
