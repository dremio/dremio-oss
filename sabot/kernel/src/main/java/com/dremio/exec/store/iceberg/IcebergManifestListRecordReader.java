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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.RecordReader;
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

  public IcebergManifestListRecordReader(OperatorContext context,
                                         FileSystem dfs,
                                         EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
                                         List<SchemaPath> columns,
                                         Configuration fsConf) {
    this.splitAttributes = splitAttributes;
    this.context = context;
    this.fsConf = fsConf;
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
          outIndex++;
        }
      }
      vector.setValueCount(outIndex);
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to read from manifest list file.")
        .build(logger);
    }
    return outIndex;
  }

  @Override
  public void close() throws Exception {
  }
}
