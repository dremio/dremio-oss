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

import java.io.IOException;
import java.util.List;

import org.apache.iceberg.io.SeekableInputStream;

import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;

public interface SeekableInputStreamFactory {

  String KEY = "dremio.plugins.iceberg.manifests.input_stream_factory";

  SeekableInputStream getStream(FileSystem fs, OperatorContext context, Path path, Long fileLength, Long mtime,
                                List<String> dataset, String datasourcePluginUID) throws IOException;

  SeekableInputStreamFactory DEFAULT = (fs, context, path, fileLength, mtime, dataset, pluginUID) -> wrap(fs.open(path));

  static SeekableInputStream wrap(FSInputStream is) {
    return new SeekableInputStream() {
      @Override
      public long getPos() throws IOException {
        return is.getPosition();
      }

      @Override
      public void seek(long newPos) throws IOException {
        is.setPosition(newPos);
      }

      @Override
      public int read() throws IOException {
        return is.read();
      }

      @Override
      public int read(byte[] b) throws IOException {
        return is.read(b);
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        return is.read(b, off, len);
      }

      @Override
      public void close() throws IOException {
        is.close();
      }
    };
  }
}
