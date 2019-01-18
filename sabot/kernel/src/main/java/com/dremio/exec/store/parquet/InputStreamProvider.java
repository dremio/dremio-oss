/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Provides input streams with one of two policies: reuse a single stream, or create a new stream each time stream() is called
 * For the case where streams are reused, users must handle the repositioning of the stream
 */
public class InputStreamProvider implements AutoCloseable {

  private final FileSystem fs;
  private final Path path;
  private final boolean singleStream;

  private final List<FSDataInputStream> streams = new ArrayList<>();

  public InputStreamProvider(FileSystem fs, Path path, boolean singleStream) {
    this.fs = fs;
    this.path = path;
    this.singleStream = singleStream;
  }

  public FSDataInputStream stream() throws IOException {
    if (singleStream) {
      if (streams.isEmpty()) {
        FSDataInputStream stream = fs.open(path);
        streams.add(stream);
      }
      return streams.get(0);
    }

    FSDataInputStream stream = fs.open(path);
    streams.add(stream);
    return stream;
  }

  public boolean singleStream() {
    return singleStream;
  }

  public void close() throws IOException {
    for (FSDataInputStream stream : streams) {
      stream.close();
    }
  }
}
