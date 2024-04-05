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
package com.dremio.exec.util.iceberg;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/** Simple appender for writing parquet files. Currently only supports row count metrics. */
public class SimpleParquetAppender<T> implements FileAppender<T> {

  private ParquetWriter<T> writer;
  private OutputFile outputFile;
  private ParquetMetadata footer;
  private long fileSize;

  public SimpleParquetAppender(ParquetWriter<T> writer, OutputFile outputFile) {
    this.writer = writer;
    this.outputFile = outputFile;
  }

  @Override
  public void add(T datum) {
    try {
      writer.write(datum);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write record " + datum, e);
    }
  }

  @Override
  public Metrics metrics() {
    final Long rowCount = footer.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
    return new Metrics(rowCount, null, null, null, null);
  }

  @Override
  public long length() {
    return fileSize;
  }

  @Override
  public List<Long> splitOffsets() {
    return footer.getBlocks().stream()
        .map(BlockMetaData::getStartingPos)
        .sorted()
        .collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      fileSize = outputFile.toInputFile().getLength();
      footer = writer.getFooter();
      writer = null;
      outputFile = null;
    }
  }
}
