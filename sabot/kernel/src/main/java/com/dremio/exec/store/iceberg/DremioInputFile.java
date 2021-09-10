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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * DremioInputFile is a dremio implementation of Iceberg InputFile interface.
 * It uses Dremio implementation of FileSystem to leverage several existing
 * functionalities like tracking IO time, Caching, Async read aheads where necessary.
 */
public class DremioInputFile implements InputFile {

  private final FileSystem fs;
  private final Path path;
  private final Long fileSize;
  private final OperatorContext context;
  private final List<String> dataset;
  private final String datasourcePluginUID; // this can be null if data files, metadata file can be accessed with same plugin
  private final String locationWithScheme;
  private final HadoopInputFile hadoopInputFile;

  public DremioInputFile(FileSystem fs, Path path, Long fileSize, OperatorContext context, List<String> dataset,
                         String datasourcePluginUID, Configuration conf) {
    this.fs = fs;
    this.path = path;
    this.fileSize = fileSize;
    this.context = context;
    this.dataset = dataset;
    this.datasourcePluginUID = datasourcePluginUID; // this can be null if it is same as the plugin which created fs
    String scheme = fs != null ? fs.getScheme() : DremioHadoopUtils.getHadoopFSScheme(
      DremioHadoopUtils.toHadoopPath(path),
      conf);
    this.locationWithScheme = IcebergUtils.getValidIcebergPath(new org.apache.hadoop.fs.Path(path.toString()), conf, scheme);
    this.hadoopInputFile = HadoopInputFile.fromLocation(this.path.toString(), conf);
  }

  @Override
  public long getLength() {
    return fileSize != null? fileSize : hadoopInputFile.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      if(context != null && fs != null) {
        SeekableInputStreamFactory factory = context.getConfig().getInstance(SeekableInputStreamFactory.KEY, SeekableInputStreamFactory.class, SeekableInputStreamFactory.DEFAULT);
        return factory.getStream(fs, context, path,
          fileSize, /* Since manifest avro files are immutable, we are passing 0 as mtime */ 0, dataset, datasourcePluginUID);
      } else {
        return hadoopInputFile.newStream();
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to open a new stream for file : %s", path), e);
    }
  }

  @Override
  public String location() {
    return locationWithScheme;
  }

  @Override
  public boolean exists() {
    try {
      return fs != null? fs.exists(path) : hadoopInputFile.exists();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to check existence of file %s", path.toString()), e);
    }
  }
}
