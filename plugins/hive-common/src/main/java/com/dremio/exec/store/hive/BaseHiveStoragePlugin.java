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
package com.dremio.exec.store.hive;

import java.io.IOException;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.AsyncStreamConf;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Base class which all hive storage plugins extend
 */
public abstract class BaseHiveStoragePlugin {
  private final SabotContext sabotContext;
  private final String name;

  protected BaseHiveStoragePlugin(SabotContext sabotContext, String pluginName) {
    this.sabotContext = sabotContext;
    this.name = pluginName;
  }

  protected String getName() {
    return name;
  }

  public FileSystem createFS(FileSystem fs, OperatorContext operatorContext, AsyncStreamConf cacheAndAsyncConf) throws IOException {
    return this.sabotContext.getFileSystemWrapper().wrap(fs, this.getName(), cacheAndAsyncConf,
        operatorContext, cacheAndAsyncConf.isAsyncEnabled(), false);
  }
}
