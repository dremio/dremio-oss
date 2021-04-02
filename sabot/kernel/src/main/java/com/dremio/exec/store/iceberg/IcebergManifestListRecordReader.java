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

import static com.dremio.exec.store.iceberg.IcebergUtils.getValueFromByteBuffer;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * Manifest list record reader
 */
public class IcebergManifestListRecordReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergManifestListRecordReader.class);

  private final EasyProtobuf.EasyDatasetSplitXAttr splitAttributes;
  private OutputMutator output;
  private Table table;
  private Iterator<ManifestFile> manifestFileIterator;
  private final OperatorContext context;
  private final Configuration fsConf;
  private final BatchSchema schema;
  private final List<String> partitionCols;
  private final Map<String, Integer> partColToKeyMap;

  public IcebergManifestListRecordReader(OperatorContext context,
                                         FileSystem dfs,
                                         EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
                                         List<SchemaPath> columns,
                                         Configuration fsConf,
                                         EasySubScan config) {
    this.splitAttributes = splitAttributes;
    this.context = context;
    this.fsConf = fsConf;
    this.schema = config.getFullSchema();
    this.partitionCols = config.getPartitionColumns();
    this.partColToKeyMap = partitionCols != null ? IntStream.range(0, partitionCols.size())
      .boxed()
      .collect(Collectors.toMap(i -> partitionCols.get(i).toLowerCase(), i -> i)) : null;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    table = (new HadoopTables(this.fsConf)).load(splitAttributes.getPath());
    manifestFileIterator = table.currentSnapshot().dataManifests().iterator();
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public int next() {
    int outIndex = 0;
    try {
      VarBinaryVector vector = (VarBinaryVector)output.getVector(RecordReader.SPLIT_INFORMATION);
      while (manifestFileIterator.hasNext() && outIndex < context.getTargetBatchSize()) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
          ObjectOutput out = new ObjectOutputStream(bos);
          ManifestFile manifestFile = manifestFileIterator.next();
          out.writeObject(manifestFile);
          vector.setSafe(outIndex, bos.toByteArray());

          for (Field field : schema) {
            if (field.getName().equals(RecordReader.SPLIT_INFORMATION)) {
              continue;
            }
            writeToVector(output.getVector(field.getName()), outIndex, getStatValue(manifestFile, field));
          }
          outIndex++;
        }
      }
      int valueCount = outIndex;
      output.getVectors().forEach(v -> v.setValueCount(valueCount));
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to read from manifest list file.")
        .build(logger);
    }
    return outIndex;
  }

  private Object getStatValue(ManifestFile manifestFile, Field field) {
    String fieldName = field.getName();
    Preconditions.checkArgument(fieldName.length() > 4);
    String colName = fieldName.substring(0, fieldName.length() - 4).toLowerCase();
    String suffix = fieldName.substring(fieldName.length() - 3).toLowerCase();
    Preconditions.checkArgument(partColToKeyMap.containsKey(colName), "parition column not presentx");
    int key = partColToKeyMap.get(colName);
    Object value;

    switch (suffix) {
      case "min":
        value = getValueFromByteBuffer(manifestFile.partitions().get(key).lowerBound(), field);
        break;
      case "max":
        value = getValueFromByteBuffer(manifestFile.partitions().get(key).upperBound(), field);
        break;
      default:
        throw UserException.unsupportedError().message("unexpected suffix for column: " + fieldName).buildSilently();
    }

    return value;
  }

  @Override
  public void close() throws Exception {
  }
}
