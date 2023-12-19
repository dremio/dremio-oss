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

package com.dremio.exec.store.dfs;

import java.io.IOException;

import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * A {@link FileSystemWrapper} implementation which wraps a {@link FileSystem} to enable logging of calls.
 * The wrapping is only done if WARN level logging is enabled for {@link LoggedFileSystem}.  See
 * {@link LoggedFileSystem} for details on tuning the amount of logging produced.
 */
public class LoggedFileSystemWrapper implements FileSystemWrapper {

  private final FileSystemWrapper defaultWrapper;
  private final OptionResolver globalOptions;

  public LoggedFileSystemWrapper(FileSystemWrapper defaultWrapper, OptionResolver globalOptions) {
    this.defaultWrapper = defaultWrapper;
    this.globalOptions = globalOptions;
  }

  @Override
  public FileSystem wrap(FileSystem fs, String storageId, AsyncStreamConf conf, OperatorContext context,
      boolean enableAsync, boolean isMetadataRefresh) throws IOException {
    FileSystem wrappedFs = defaultWrapper.wrap(fs, storageId, conf, context, enableAsync, isMetadataRefresh);
    if (LoggedFileSystem.isLoggingEnabled()) {
      // use options from the OperatorContext if available, otherwise fall back to global options
      OptionResolver options = context != null && context.getOptions() != null ? context.getOptions() : globalOptions;
      wrappedFs = new LoggedFileSystem(wrappedFs, options);
    }
    return wrappedFs;
  }


  /**
   * Note this does not unwrap to a FileSystem instance, it supports unwrapping on the contained FileSystemWrapper.
   * Naming is a bit confusing given unwrap is not the reverse of wrap.
   */
  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }

    return defaultWrapper.unwrap(clazz);
  }

  @Override
  public boolean isWrapperFor(Class<?> clazz) {
    return clazz.isInstance(this) || defaultWrapper.isWrapperFor(clazz);
  }

  @Override
  public void close() throws IOException {
    defaultWrapper.close();
  }
}
