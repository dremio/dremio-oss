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

import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * DremioInputFile is a dremio implementation of Iceberg InputFile interface.
 * It uses Dremio implementation of FileSystem to leverage several existing
 * functionalities like tracking IO time, Caching, Async read aheads where necessary.
 */
public class DremioInputFile implements InputFile {

  private final FileSystem fs;
  private final Path path;
  private final Long fileSize;
  private final Long mtime;
  private final OperatorContext context;
  private final List<String> dataset;
  private final String datasourcePluginUID; // this can be null if data files, metadata file can be accessed with same plugin
  private final String locationWithScheme;
  private HadoopInputFile hadoopInputFile;
  private final Configuration configuration;
  private final org.apache.hadoop.fs.FileSystem hadoopFs;
  private final String filePath;

  public DremioInputFile(FileSystem fs, Path path, Long fileSize, Long mtime, OperatorContext context, List<String> dataset,
                         String datasourcePluginUID, Configuration conf, org.apache.hadoop.fs.FileSystem hadoopFs) {
    this.fs = fs;
    this.path = path;
    this.fileSize = fileSize;
    this.mtime = mtime;
    this.context = context;
    this.dataset = dataset;
    this.datasourcePluginUID = datasourcePluginUID; // this can be null if it is same as the plugin which created fs
    this.configuration = conf;
    String scheme;
    if (fs == null) {
      Preconditions.checkArgument(hadoopFs != null, "HadoopFs can not be null");
      filePath = Path.getContainerSpecificRelativePath(path);
      scheme = hadoopFs.getScheme();
    } else {
      scheme = fs.getScheme();
      filePath = this.path.toString();
    }
    this.locationWithScheme = IcebergUtils.getValidIcebergPath(new org.apache.hadoop.fs.Path(filePath), conf, scheme);
    this.hadoopFs = hadoopFs;
  }

  private HadoopInputFile getHadoopInputFile() {
    if (hadoopInputFile != null) {
      return hadoopInputFile;
    }
    Preconditions.checkState(hadoopFs != null, "Unexpected state");
    this.hadoopInputFile = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(filePath), hadoopFs, configuration);
    return hadoopInputFile;
  }

  @Override
  public long getLength() {
    return fileSize != null? fileSize : getHadoopInputFile().getLength();
  }

  public long getVersion() {
    return mtime != null ? mtime : 0;
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      if(context != null && fs != null) {
        SeekableInputStreamFactory factory = context.getConfig().getInstance(SeekableInputStreamFactory.KEY, SeekableInputStreamFactory.class, SeekableInputStreamFactory.DEFAULT);
        return factory.getStream(fs, context, path,
          fileSize, mtime, dataset, datasourcePluginUID);
      } else {
        return getHadoopInputFile().newStream();
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
      return fs != null? fs.exists(path) : getHadoopInputFile().exists();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to check existence of file %s", path.toString()), e);
    }
  }
}
