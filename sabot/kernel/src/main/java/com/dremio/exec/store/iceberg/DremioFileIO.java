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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.DremioOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.common.util.concurrent.ContextClassLoaderSwapper;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;

/**
 * DremioFileIO is an implementation of Iceberg FileIO interface.
 * It mainly is used for returning the Dremio implementation of
 * Iceberg InputFile and Outputfile interfaces
 *
 * Calls to DremioFileIO APIs maybe done from Hive classes from a different
 * class loader context. So, always changing the context to application class loader in each method.
 */
public class DremioFileIO implements FileIO {

  private final FileSystem fs;
  private final OperatorContext context;
  private final List<String> dataset;
  private org.apache.hadoop.fs.FileSystem hadoopFs;
  private MutablePlugin plugin;

  /*
   * Send FileLength as non null if we want to use FileIO for single file read.
   * For multiple file read send fileLength as a null.
   */
  private final Long fileLength;
  private final Configuration conf;
  private final String datasourcePluginUID; // this can be null if data files, metadata file can be accessed with same plugin

  public DremioFileIO(Configuration conf, MutablePlugin plugin) {
    this(null, null, null, null, null, conf, plugin);
  }

  public DremioFileIO(FileSystem fs, Iterable<Map.Entry<String, String>> conf, MutablePlugin plugin) {
    this(fs, null, null, null, null, conf.iterator(), plugin);
  }

  public DremioFileIO(FileSystem fs, OperatorContext context, List<String> dataset, String datasourcePluginUID, Long fileLength, Configuration conf, MutablePlugin plugin) {
    Preconditions.checkNotNull(conf, "Configuration can not be null");
    Preconditions.checkNotNull(plugin, "Plugin can not be null");
    this.fs = fs;
    this.context = context;
    this.dataset = dataset;
    this.datasourcePluginUID = datasourcePluginUID; // this can be null if it is same as the plugin which created fs
    this.fileLength = fileLength;
    this.conf = conf;
    this.plugin = plugin;
  }

  private DremioFileIO(FileSystem fs, OperatorContext context, List<String> dataset, String datasourcePluginUID, Long fileLength, Iterator<Map.Entry<String, String>> conf, MutablePlugin plugin) {
    try (Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(DremioFileIO.class)) {
      Preconditions.checkNotNull(conf, "Configuration can not be null");
      Preconditions.checkNotNull(plugin, "Plugin can not be null");
      this.fs = fs;
      this.context = context;
      this.dataset = dataset;
      this.datasourcePluginUID = datasourcePluginUID; // this can be null if it is same as the plugin which created fs
      this.fileLength = fileLength;
      this.conf = new Configuration();
      while (conf.hasNext()) {
        Map.Entry<String, String> property = conf.next();
        this.conf.set(property.getKey(), property.getValue());
      }
      this.plugin = plugin;
    }
  }



  // In case if FS is null then reading of file will be take care by HadoopInputFile.
  @Override
  public InputFile newInputFile(String path) {
    try (Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(DremioFileIO.class)) {
      Long fileSize;
      Long mtime = 0L;
      Path filePath = Path.of(path);
      if (fs != null && !fs.supportsPathsWithScheme()) {
        path = Path.getContainerSpecificRelativePath(filePath);
        filePath = Path.of(path);
      }

      if (fileLength == null && fs != null) {
        try {
          FileAttributes fileAttributes = fs.getFileAttributes(filePath);
          fileSize = fileAttributes.size();
          mtime = fileAttributes.lastModifiedTime().toMillis();
        } catch (FileNotFoundException e) {
          // ignore if file not found, it is valid to create an InputFile for a file that does not exist
          fileSize = null;
          mtime = null;
        }
      } else {
        fileSize = fileLength;
      }

      initializeHadoopFs(filePath);
      return new DremioInputFile(fs, filePath, fileSize, mtime, context, dataset, datasourcePluginUID, conf, hadoopFs);
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  private void initializeHadoopFs(Path path) {
    // initialize hadoop Fs firstTime
    if(hadoopFs == null && (context == null || fs == null)) {
      hadoopFs = plugin.getHadoopFsSupplier(path.toString(), conf).get();
    }
  }

  @Override
  public OutputFile newOutputFile(String path) {
    try (Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(DremioFileIO.class)) {
      if (fs == null || !fs.supportsPathsWithScheme()) {
        path = Path.getContainerSpecificRelativePath(Path.of(path));
      }
      initializeHadoopFs(Path.of(path));
      return new DremioOutputFile(path, conf, hadoopFs);
    }
  }

  @Override
  public void deleteFile(String path) {
    deleteFile(path, false /* not recursive */, true);
  }

  public void deleteFile(String path, boolean recursive, boolean getContainerSpecificRelativePath) {
    try (Closeable swapper = ContextClassLoaderSwapper.swapClassLoader(DremioFileIO.class)) {
      if ((fs == null || !fs.supportsPathsWithScheme()) && getContainerSpecificRelativePath) {
        path = Path.getContainerSpecificRelativePath(Path.of(path));
      }
      org.apache.hadoop.fs.Path toDelete = DremioHadoopUtils.toHadoopPath(path);
      org.apache.hadoop.fs.FileSystem fs = plugin.getHadoopFsSupplier(toDelete.toString(), conf).get();
      try {
        fs.delete(toDelete, recursive );
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to delete file: %s", path);
      }
    }
  }

  public MutablePlugin getPlugin(){
    return this.plugin;
  }
}
