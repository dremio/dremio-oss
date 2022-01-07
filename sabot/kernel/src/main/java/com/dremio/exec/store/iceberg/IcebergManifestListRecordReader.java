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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION;
import static com.dremio.exec.store.iceberg.IcebergUtils.getValueFromByteBuffer;
import static com.dremio.exec.store.iceberg.IcebergUtils.isNonAddOnField;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeSplitIdentity;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.types.Type;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * Manifest list record reader
 */
public class IcebergManifestListRecordReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergManifestListRecordReader.class);

  private final BatchSchema schema;
  private final EasyProtobuf.EasyDatasetSplitXAttr splitAttributes;
  private final List<String> dataset;
  private OutputMutator output;
  private Iterator<ManifestFile> manifestFileIterator;
  private final OperatorContext context;
  private final List<String> partitionCols;
  private final Map<String, Integer> partColToKeyMap;
  private final IcebergExtendedProp icebergExtendedProp;

  private Schema icebergTableSchema;
  private byte[] icebergDatasetXAttr;
  private final SupportsIcebergRootPointer pluginForIceberg;
  private final OpProps props;
  private ArrowBuf tmpBuf;
  private boolean emptyTable;
  private final String datasourcePluginUID;
  private Expression icebergFilterExpression;
  private Map<Integer, PartitionSpec> partitionSpecMap;

  public IcebergManifestListRecordReader(OperatorContext context,
                                         FileSystem fileSystem,
                                         EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
                                         SupportsIcebergRootPointer pluginForIceberg,
                                         EasySubScan config) {
    this.splitAttributes = splitAttributes;
    this.context = context;
    this.pluginForIceberg = pluginForIceberg;
    this.dataset = config.getReferencedTables() != null ? config.getReferencedTables().iterator().next() : null;
    this.datasourcePluginUID = config.getDatasourcePluginId().getName();
    this.schema = config.getFullSchema();
    this.props = config.getProps();
    this.partitionCols = config.getPartitionColumns();
    this.partColToKeyMap = partitionCols != null ? IntStream.range(0, partitionCols.size())
      .boxed()
      .collect(Collectors.toMap(i -> partitionCols.get(i).toLowerCase(), i -> i)) : null;
    icebergExtendedProp = config.getIcebergExtendedProp();
    try {
      this.icebergFilterExpression = IcebergSerDe.deserializeFromByteArray(icebergExtendedProp.getIcebergExpression());
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize Iceberg Expression");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize Iceberg Expression", e);
    }
  }

  private Map<Integer, PartitionSpec> getPartitionSpecMap(IcebergExtendedProp icebergExtendedProp, TableMetadata tableMetadata ) {
    if (icebergExtendedProp == null ||
      icebergExtendedProp.getPartitionSpecs() == null) {
      return tableMetadata.specs().stream().collect(Collectors.toMap(f-> f.specId(), f->f));
    }
    return IcebergSerDe.deserializePartitionSpecMap(icebergExtendedProp.getPartitionSpecs().toByteArray());
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    FileSystem fs;
    try {
      fs = pluginForIceberg.createFSWithAsyncOptions(splitAttributes.getPath(), props.getUserName(), context);
    } catch (IOException e) {
      throw new RuntimeException("Failed creating filesystem", e);
    }
    TableMetadata tableMetadata = TableMetadataParser.read(new DremioFileIO(
            fs, context, dataset, datasourcePluginUID, null, pluginForIceberg.getFsConfCopy()),
            splitAttributes.getPath());
    if (!context.getOptions().getOption(ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION)) {
      checkForPartitionSpecEvolution(tableMetadata);
    }
    partitionSpecMap = getPartitionSpecMap(icebergExtendedProp, tableMetadata);
    icebergTableSchema = tableMetadata.schema();
    Snapshot snapshot = tableMetadata.currentSnapshot();
    if (snapshot == null) {
      emptyTable = true;
      return;
    }
    List<ManifestFile> manifestFileList = snapshot.dataManifests();
    manifestFileList = filterManifestFiles(manifestFileList);
    manifestFileIterator = manifestFileList.iterator();
    icebergDatasetXAttr = IcebergProtobuf.IcebergDatasetXAttr.newBuilder()
      .addAllColumnIds(IcebergUtils.getIcebergColumnNameToIDMap(icebergTableSchema).entrySet().stream()
        .map(c -> IcebergProtobuf.IcebergSchemaField.newBuilder()
          .setSchemaPath(c.getKey())
          .setId(c.getValue())
          .build())
        .collect(Collectors.toList()))
      .build()
      .toByteArray();
    tmpBuf = context.getAllocator().buffer(4096);
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public int next() {
    if (emptyTable) {
      return 0;
    }

    int outIndex = 0;
    try {
      StructVector splitIdentityVector = (StructVector) output.getVector(RecordReader.SPLIT_IDENTITY);
      NullableStructWriter splitIdentityWriter = splitIdentityVector.getWriter();
      VarBinaryVector splitInfoVector = (VarBinaryVector)output.getVector(RecordReader.SPLIT_INFORMATION);
      VarBinaryVector colIdsVector = (VarBinaryVector)output.getVector(RecordReader.COL_IDS);
      while (manifestFileIterator.hasNext() && outIndex < context.getTargetBatchSize()) {
        ManifestFile manifestFile = manifestFileIterator.next();
        SplitIdentity splitIdentity = new SplitIdentity(manifestFile.path(), 0, manifestFile.length(), manifestFile.length());

        writeSplitIdentity(splitIdentityWriter, outIndex, splitIdentity, tmpBuf);
        splitInfoVector.setSafe(outIndex, IcebergSerDe.serializeToByteArray(manifestFile));
        colIdsVector.setSafe(outIndex, icebergDatasetXAttr);

        for (Field field : schema) {
          if (isNonAddOnField(field.getName())) {
            continue;
          }
          writeToVector(output.getVector(field.getName()), outIndex, getStatValue(manifestFile, field));
        }
        outIndex++;
      }
      int valueCount = outIndex;
      output.getVectors().forEach(v -> v.setValueCount(valueCount));
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Unable to read manifest list files for table '%s'", PathUtils.constructFullPath(dataset))
        .build(logger);
    }
    return outIndex;
  }

  private Object getStatValue(ManifestFile manifestFile, Field field) {
    String fieldName = field.getName();
    Preconditions.checkArgument(fieldName.length() > 4);
    String colName = fieldName.substring(0, fieldName.length() - 4).toLowerCase();
    String suffix = fieldName.substring(fieldName.length() - 3).toLowerCase();
    Preconditions.checkArgument(partColToKeyMap.containsKey(colName), "partition column not present");
    int key = partColToKeyMap.get(colName);
    Type fieldType = icebergTableSchema.caseInsensitiveFindField(colName).type();
    Object value;

    switch (suffix) {
      case "min":
        value = key < manifestFile.partitions().size() ? getValueFromByteBuffer(manifestFile.partitions().get(key).lowerBound(), fieldType) : null;
        break;
      case "max":
        value = key < manifestFile.partitions().size() ? getValueFromByteBuffer(manifestFile.partitions().get(key).upperBound(), fieldType) : null;
        break;
      default:
        throw UserException.unsupportedError().message("unexpected suffix for column: " + fieldName).buildSilently();
    }

    return value;
  }

  private void checkForPartitionSpecEvolution(TableMetadata tableMetadata) {
    if (tableMetadata.specs().size() > 1) {
      throw UserException
              .unsupportedError()
              .message("Iceberg tables with partition spec evolution are not supported")
              .buildSilently();
    }

    if (checkNonIdentityTransform(tableMetadata.spec())) {
      throw UserException
              .unsupportedError()
              .message("Iceberg tables with Non-identity partition transforms are not supported")
              .buildSilently();
    }
  }

  private boolean checkNonIdentityTransform(PartitionSpec partitionSpec) {
    return partitionSpec.fields().stream().anyMatch(partitionField -> !partitionField.transform().isIdentity());
  }

  @Override
  public void close() throws Exception {
    context.getStats().setReadIOStats();
    AutoCloseables.close(tmpBuf);
  }

  private List<ManifestFile> filterManifestFiles(List<ManifestFile> manifestFileList) {
    if (icebergFilterExpression == null) {
      return manifestFileList;
    }
    Map<Integer, ManifestEvaluator> evaluatorMap = new HashMap<>();
    for (Map.Entry<Integer, PartitionSpec> spec : partitionSpecMap.entrySet()) {
      ManifestEvaluator partitionEval = ManifestEvaluator.forRowFilter(icebergFilterExpression, spec.getValue(), false);
      evaluatorMap.put(spec.getKey(), partitionEval);
    }
    return manifestFileList.stream().filter(file -> evaluatorMap.get(file.partitionSpecId()).eval(file)).collect(Collectors.toList());
  }
}
