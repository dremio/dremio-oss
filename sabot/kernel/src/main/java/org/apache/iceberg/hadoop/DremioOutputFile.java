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
package org.apache.iceberg.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.exec.store.iceberg.IcebergUtils;

/**
 * DremioOutputFile. used in DremioFileIO to output iceberg metadata file. wrapper for HadoopOutputFile.
 */
public class DremioOutputFile implements OutputFile {

  private final Path path;
  private final String locationWithScheme;
  private final OutputFile hadoopOutputFile;

  public DremioOutputFile(String path, Configuration conf) {
    hadoopOutputFile = HadoopOutputFile.fromLocation(path, conf);
    this.path = new Path(path);
    this.locationWithScheme = IcebergUtils.getValidIcebergPath(this.path, conf, DremioHadoopUtils.getHadoopFSScheme(this.path, conf));
  }

  @Override
  public PositionOutputStream create() {
    return hadoopOutputFile.create();
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return hadoopOutputFile.createOrOverwrite();
  }

  @Override
  public String location() {
    return locationWithScheme;
  }

  @Override
  public InputFile toInputFile() {
    return hadoopOutputFile.toInputFile();
  }

  @Override
  public String toString() {
    return location();
  }
}
