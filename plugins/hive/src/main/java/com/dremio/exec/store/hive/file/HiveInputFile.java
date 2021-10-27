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

package com.dremio.exec.store.hive.file;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * InputFile implementation for reading iceberg table metadata.
 */
class HiveInputFile implements InputFile {
  private final HadoopInputFile hadoopInputFile;

  public HiveInputFile(String location, Configuration conf) {
    //Switching off the caching so that the DistributedFileSystem is used for the metadataLocation only for this.
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", URI.create(location).getScheme()), true);
    this.hadoopInputFile = HadoopInputFile.fromLocation(location, conf);
  }

  @Override
  public long getLength() {
    return hadoopInputFile.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    return hadoopInputFile.newStream();
  }

  @Override
  public String location() {
    return hadoopInputFile.location();
  }

  @Override
  public boolean exists() {
    return hadoopInputFile.exists();
  }
}
