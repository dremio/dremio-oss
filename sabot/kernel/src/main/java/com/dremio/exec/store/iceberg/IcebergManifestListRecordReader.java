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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN_WITH_EQUALITY_DELETE;
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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.ManifestContent;
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
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;

/**
 * Manifest list record reader
 */
public class IcebergManifestListRecordReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergManifestListRecordReader.class);

  private final BatchSchema schema;
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
  private final String path;
  private final ManifestContent manifestContent;
  private Map<Integer, PartitionSpec> partitionSpecMap;

  public IcebergManifestListRecordReader(OperatorContext context,
                                         String path,
                                         SupportsIcebergRootPointer pluginForIceberg,
                                         List<String> dataset,
                                         String dataSourcePluginId,
                                         BatchSchema fullSchema,
                                         OpProps props,
                                         List<String> partitionCols,
                                         IcebergExtendedProp icebergExtendedProp,
                                         ManifestContent manifestContent) {
    this.path = path;
    this.context = context;
    this.pluginForIceberg = pluginForIceberg;
    this.dataset = dataset;
    this.datasourcePluginUID = dataSourcePluginId;
    this.schema = fullSchema;
    this.props = props;
    this.partitionCols = partitionCols;
    this.partColToKeyMap = partitionCols != null ? IntStream.range(0, partitionCols.size())
      .boxed()
      .collect(Collectors.toMap(i -> partitionCols.get(i).toLowerCase(), i -> i)) : null;
    this.icebergExtendedProp = icebergExtendedProp;
    this.manifestContent = manifestContent;
    try {
      this.icebergFilterExpression = IcebergSerDe.deserializeFromByteArray(icebergExtendedProp.getIcebergExpression());
    } catch (IOException e) {
      throw new RuntimeIOException(e, "failed to deserialize Iceberg Expression");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize Iceberg Expression", e);
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    FileSystem fs;
    try {
      fs = pluginForIceberg.createFSWithAsyncOptions(this.path, props.getUserName(), context);
    } catch (IOException e) {
      throw new RuntimeException("Failed creating filesystem", e);
    }
    TableMetadata tableMetadata = TableMetadataParser.read(new DremioFileIO(
            fs, context, dataset, datasourcePluginUID, null, pluginForIceberg.getFsConfCopy(), (MutablePlugin) pluginForIceberg),
            this.path);
    if (!context.getOptions().getOption(ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION)) {
      checkForPartitionSpecEvolution(tableMetadata);
    }
    partitionSpecMap = tableMetadata.specsById();
    final long snapshotId = icebergExtendedProp.getSnapshotId();
    final Snapshot snapshot = snapshotId == -1 ? tableMetadata.currentSnapshot() : tableMetadata.snapshot(snapshotId);
    if (snapshot == null) {
      emptyTable = true;
      return;
    }
    icebergTableSchema = icebergExtendedProp.getIcebergSchema() != null ?
            IcebergSerDe.deserializedJsonAsSchema(icebergExtendedProp.getIcebergSchema()) :
            tableMetadata.schema();

    // DX-41522, throw exception when DeleteFiles are present.
    // TODO: remove this throw when we get full support for handling the deletes correctly.
    if (tableMetadata.formatVersion() > 1) {
      long numDeleteFiles = Long.parseLong(snapshot.summary().getOrDefault("total-delete-files", "0"));
      if (numDeleteFiles > 0 && !context.getOptions().getOption(ENABLE_ICEBERG_MERGE_ON_READ_SCAN)) {
        throw UserException.unsupportedError()
            .message("Iceberg V2 tables with delete files are not supported.")
            .buildSilently();
      }
    }

    long numEqualityDeletes = Long.parseLong(snapshot.summary().getOrDefault("total-equality-deletes", "0"));
    if (numEqualityDeletes > 0 && !context.getOptions().getOption(ENABLE_ICEBERG_MERGE_ON_READ_SCAN_WITH_EQUALITY_DELETE)) {
      throw UserException.unsupportedError()
        .message("Iceberg V2 tables with equality deletes are not supported.")
        .buildSilently();
    }

    List<ManifestFile> manifestFileList = manifestContent == ManifestContent.DELETES ?
      snapshot.deleteManifests() : snapshot.dataManifests();
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

    if (IcebergUtils.checkNonIdentityTransform(tableMetadata.spec())) {
      throw UserException
              .unsupportedError()
              .message("Iceberg tables with Non-identity partition transforms are not supported")
              .buildSilently();
    }
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
