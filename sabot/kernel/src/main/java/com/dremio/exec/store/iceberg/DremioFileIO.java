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
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.DremioOutputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * DremioFileIO is an implementation of Iceberg FileIO interface.
 * It mainly is used for returning the Dremio implementation of
 * Iceberg InputFile and Outputfile interfaces
 */
public class DremioFileIO implements FileIO {

  private final FileSystem fs;
  private final OperatorContext context;
  private final List<String> dataset;

  /*
   * Send FileLength as non null if we want to use FileIO for single file read.
   * For multiple file read send fileLength as a null.
   */
  private final Long fileLength;
  private final Configuration conf;
  private final String datasourcePluginUID; // this can be null if data files, metadata file can be accessed with same plugin

  public DremioFileIO(Configuration conf) {
    this(null, null, null, null, null, conf);
  }

  public DremioFileIO(FileSystem fs, OperatorContext context, List<String> dataset, String datasourcePluginUID, Long fileLength, Configuration conf) {
    Preconditions.checkNotNull(conf, "Configuration can not be null");
    this.fs = fs;
    this.context = context;
    this.dataset = dataset;
    this.datasourcePluginUID = datasourcePluginUID; // this can be null if it is same as the plugin which created fs
    this.fileLength = fileLength;
    this.conf = conf;
  }

  // In case if FS is null then reading of file will be take care by HadoopInputFile.
  @Override
  public InputFile newInputFile(String path) {
    try {
      Long fileSize;
      Path filePath = Path.of(path);
      if (fs != null && !fs.supportsPathsWithScheme()) {
        path = Path.getContainerSpecificRelativePath(filePath);
        filePath = Path.of(path);
      }
      if (fileLength == null && fs != null) {
        fileSize = fs.getFileAttributes(filePath).size();
      } else {
        fileSize = fileLength;
      }
      return new DremioInputFile(fs, filePath, fileSize, context, dataset, datasourcePluginUID, conf);
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  @Override
  public OutputFile newOutputFile(String path) {
    path = Path.getContainerSpecificRelativePath(Path.of(path));
    return new DremioOutputFile(path, conf);
  }

  @Override
  public void deleteFile(String path) {
    path = Path.getContainerSpecificRelativePath(Path.of(path));
    org.apache.hadoop.fs.Path toDelete = DremioHadoopUtils.toHadoopPath(path);
    org.apache.hadoop.fs.FileSystem fs = Util.getFs(toDelete, conf);
    try {
      fs.delete(toDelete, false /* not recursive */);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
  }
}
