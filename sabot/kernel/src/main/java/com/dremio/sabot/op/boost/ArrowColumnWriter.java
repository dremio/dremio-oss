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
package com.dremio.sabot.op.boost;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.easy.arrow.ArrowFlatBufRecordWriter;
import com.dremio.io.AsyncByteReader;
import com.dremio.io.file.BoostedFileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowColumnWriter implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ArrowColumnWriter.class);

  public final int rowGroupIndex;
  public final String column;
  public final String splitPath;
  public final RecordWriter recordWriter;
  public final AsyncByteReader.FileKey fileKey;
  public final VectorContainer outputVectorContainer;
  public boolean isClosed = false;
  private BoostedFileSystem boostedFS;

  public ArrowColumnWriter(
      ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr,
      String column,
      OperatorContext context,
      BoostedFileSystem boostedFS,
      List<String> dataset)
      throws IOException {
    this.column = column;
    this.splitPath = splitXAttr.getPath();
    this.rowGroupIndex = splitXAttr.getRowGroupIndex();
    this.fileKey =
        AsyncByteReader.FileKey.of(
            Path.of(splitPath),
            Long.toString(splitXAttr.getLastModificationTime()),
            AsyncByteReader.FileKey.FileType.OTHER,
            dataset);
    this.boostedFS = boostedFS;
    recordWriter =
        new ArrowFlatBufRecordWriter(
            context, boostedFS.createBoostFile(fileKey, rowGroupIndex, column));
    this.outputVectorContainer = context.createOutputVectorContainer();
  }

  public void setup(ValueVector columnVector) throws IOException {
    // container with just one column
    outputVectorContainer.add(columnVector);
    outputVectorContainer.buildSchema();
    RecordWriter.WriteStatsListener byteCountListener = (b) -> {};
    RecordWriter.OutputEntryListener fileWriteListener =
        (a, b, c, d, e, f, g, partition, h, p, r) -> {};
    recordWriter.setup(outputVectorContainer, fileWriteListener, byteCountListener);
  }

  public void write(int records) throws Exception {
    outputVectorContainer.setRecordCount(records);
    recordWriter.writeBatch(0, records);
  }

  @Override
  public void close() throws Exception {
    if (!isClosed) {
      AutoCloseables.close(recordWriter); // file write is done on closing RecordWriter
      isClosed = true;
    }
  }

  public void commit() throws IOException {
    Preconditions.checkArgument(
        isClosed, "Attempted to commit boost file before finishing writing");
    boostedFS.commitBoostFile(fileKey, rowGroupIndex, column);
  }

  public void abort() {
    try {
      AutoCloseables.close(recordWriter);
      boostedFS.abortBoostFile(fileKey, rowGroupIndex, column);
    } catch (Exception ex) {
      logger.error(
          "Failure while cancelling boosting of column [{}] of split [{}]", column, splitPath, ex);
    }
  }
}
