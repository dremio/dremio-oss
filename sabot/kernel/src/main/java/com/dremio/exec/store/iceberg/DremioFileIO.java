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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.dfs.FileSystemConfigurationAdapter;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.util.List;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * An implementation of Iceberg's FileIO interface that delegates to a Dremio FileSystem instance
 * for all IO operations.
 */
public class DremioFileIO implements FileIO {

  private final FileSystem fs;
  private final OperatorContext context;
  private final List<String> dataset;

  /*
   * Send FileLength as non-null if we want to use FileIO for single file read.
   * For multiple file read send fileLength as a null.
   */
  private final Long fileLength;
  private final FileSystemConfigurationAdapter conf;
  private final String
      datasourcePluginUID; // this can be null if data files, metadata file can be accessed with
  // same plugin

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioFileIO.class);

  public DremioFileIO(
      FileSystem fs,
      OperatorContext context,
      List<String> dataset,
      String datasourcePluginUID,
      Long fileLength,
      FileSystemConfigurationAdapter conf) {
    this.fs = Preconditions.checkNotNull(fs);
    this.context = context;
    this.dataset = dataset;
    this.datasourcePluginUID =
        datasourcePluginUID; // this can be null if it is same as the plugin which created fs
    this.fileLength = fileLength;
    this.conf = Preconditions.checkNotNull(conf);
  }

  @Override
  public InputFile newInputFile(String path) {
    try {
      Long fileSize;
      Long mtime = 0L;
      Path pluginRelativePath = Path.of(path);
      if (!fs.supportsPathsWithScheme()) {
        path = Path.getContainerSpecificRelativePath(pluginRelativePath);
        pluginRelativePath = Path.of(path);
      }

      if (fileLength == null) {
        try {
          FileAttributes fileAttributes = fs.getFileAttributes(pluginRelativePath);
          fileSize = fileAttributes.size();
          mtime = fileAttributes.lastModifiedTime().toMillis();
        } catch (FileNotFoundException e) {
          // ignore if file not found, it is valid to create an InputFile for a file that does not
          // exist
          fileSize = null;
          mtime = null;
        }
      } else {
        fileSize = fileLength;
      }

      return new DremioInputFile(this, pluginRelativePath, fileSize, mtime, conf);
    } catch (AccessDeniedException e) {
      throw UserException.permissionError(e).message("Access denied on %s", path).buildSilently();
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).message("Unable to read %s", path).buildSilently();
    }
  }

  @Override
  public OutputFile newOutputFile(String path) {
    String pluginRelativePath = path;
    if (!fs.supportsPathsWithScheme()) {
      pluginRelativePath = Path.getContainerSpecificRelativePath(Path.of(pluginRelativePath));
    }
    return new DremioOutputFile(this, Path.of(pluginRelativePath), conf);
  }

  @Override
  public void deleteFile(String path) {
    if (!fs.supportsPathsWithScheme()) {
      path = Path.getContainerSpecificRelativePath(Path.of(path));
    }
    try {
      fs.delete(Path.of(path), false);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to delete file: %s", path), e);
    }
  }

  public FileSystem getFs() {
    return fs;
  }

  OperatorContext getContext() {
    return context;
  }

  List<String> getDataset() {
    return dataset;
  }

  String getDatasourcePluginUID() {
    return datasourcePluginUID;
  }
}
