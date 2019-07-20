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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;

/**
 * Interface to create FileSystemWrappers.  Any user looking to decorate file-system-wrappers with additional
 * functionality can implement this interface and override the default behaviour.
 */
public interface FileSystemWrapperCreator extends AutoCloseable {
  String FILE_SYSTEM_WRAPPER_CREATOR_CLASS = "dremio.filesystem.wrapper.creator.class";

  FileSystemWrapperCreator DEFAULT_INSTANCE = new FileSystemWrapperCreator() {};

  /**
   * Combination of operatorContext, pluginUID, pluginConfig, and fsConf should provide all information to the
   * implementation regarding the context in which the FileSystemWrapper is being requested.
   */
  default FileSystemWrapper get(
    OperatorContext operatorContext,
    String pluginUID,
    FileSystemConf pluginConfig,
    Configuration fsConf,
    List<String> uniqueConnectionProperties,
    boolean enableAsync) throws IOException {
    return get(fsConf, operatorContext, uniqueConnectionProperties, enableAsync);
  }

  default void close() throws Exception {
    // Nothing to do in the default implementation
  }

  static FileSystemWrapper get(Configuration fsConf) throws IOException {
    return new FileSystemWrapper(fsConf);
  }

  static FileSystemWrapper get(URI uri, Configuration fsConf, boolean enableAsync) throws IOException {
    FileSystem fs = FileSystem.get(uri, fsConf);
    return new FileSystemWrapper(fsConf, fs, enableAsync);
  }

  static FileSystemWrapper get(Path path, Configuration fsConf) throws IOException {
    FileSystem fs = path.getFileSystem(fsConf);
    return new FileSystemWrapper(fsConf, fs);
  }

  static FileSystemWrapper get(Configuration fsConf, OperatorStats stats) throws IOException {
    return get(fsConf, stats, false);
  }

  static FileSystemWrapper get(Configuration fsConf, OperatorStats stats, boolean enableAsync) throws IOException {
    return new FileSystemWrapper(fsConf, stats, null, enableAsync);
  }

  static FileSystemWrapper get(Configuration fsConf, OperatorStats stats, List<String> uniqueConnectionProperties,
                               boolean enableAsync) throws IOException {
    return new FileSystemWrapper(fsConf, stats, uniqueConnectionProperties, enableAsync);
  }

  static FileSystemWrapper get(Configuration fsConf, OperatorContext operatorContext,
                               List<String> uniqueConnectionProperties, boolean enableAsync) throws IOException {
    return new FileSystemWrapper(fsConf, (operatorContext == null) ? null : operatorContext.getStats(),
      uniqueConnectionProperties, enableAsync);
  }

  static FileSystemWrapper get(Path path, Configuration fsConf, OperatorStats stats) throws IOException {
    return get(path, fsConf, stats, false);
  }

  static FileSystemWrapper get(Path path, Configuration fsConf, OperatorStats stats, boolean enableAsync) throws IOException {
    FileSystem fs = path.getFileSystem(fsConf);
    return new FileSystemWrapper(fsConf, fs, stats, enableAsync);
  }

  static FileSystemWrapper get(URI uri, Configuration fsConf, OperatorStats stats) throws IOException {
    FileSystem fs = FileSystem.get(uri, fsConf);
    return new FileSystemWrapper(fsConf, fs, stats, false);
  }
}
