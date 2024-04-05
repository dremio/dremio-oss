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
package com.dremio.exec.hadoop;

import com.dremio.io.CompressedFSInputStream;
import com.dremio.io.CompressionCodec;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.FSInputStream;
import com.dremio.io.file.Path;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;

/** Hadoop implementation of {@code CompressionCodecFactory} */
public class HadoopCompressionCodecFactory implements CompressionCodecFactory {
  private static final class CompressionCodecWrapper implements CompressionCodec {
    private final org.apache.hadoop.io.compress.CompressionCodec delegate;

    private CompressionCodecWrapper(org.apache.hadoop.io.compress.CompressionCodec delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompressedFSInputStream newInputStream(FSInputStream open) throws IOException {
      return new CompressionInputStreamWrapper(delegate.createInputStream(open));
    }
  }

  /** Default factory instance */
  public static final CompressionCodecFactory DEFAULT =
      new HadoopCompressionCodecFactory(new Configuration());

  private final org.apache.hadoop.io.compress.CompressionCodecFactory factory;

  public HadoopCompressionCodecFactory(Configuration configuration) {
    this.factory = new org.apache.hadoop.io.compress.CompressionCodecFactory(configuration);
  }

  @Override
  public CompressionCodec getCodec(Path path) {
    Objects.requireNonNull(path);
    final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toURI());
    final org.apache.hadoop.io.compress.CompressionCodec codec = factory.getCodec(hadoopPath);
    if (codec == null) {
      return null;
    }

    return new CompressionCodecWrapper(codec);
  }
}
