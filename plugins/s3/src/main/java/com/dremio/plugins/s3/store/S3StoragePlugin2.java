/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.plugins.s3.store;

import java.net.URI;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;

import com.dremio.exec.store.dfs.FileSystemStoragePlugin2;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.service.namespace.SourceTableDefinition;

/**
 * S3 Extension of FileSystemStoragePlugin2
 */
public class S3StoragePlugin2 extends FileSystemStoragePlugin2 {

  public S3StoragePlugin2(String name, S3PluginConfig fileSystemConfig, S3Plugin fsPlugin) {
    super(name, fileSystemConfig, fsPlugin);
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
    S3Plugin plugin = (S3Plugin) this.getFsPlugin();
    // have to do it to set correct FS_DEFAULT_NAME
    plugin.refreshState();
    final FileSystemWrapper fs = plugin.getFS(ImpersonationUtil.getProcessUserName());
    // do not use fs.getURI() or fs.getConf() directly as they will produce wrong results
    fs.initialize(URI.create(plugin.getFsConf().get(FileSystem.FS_DEFAULT_NAME_KEY)), plugin.getFsConf());
    return Collections.emptyList(); // file system does not know about physical datasets
  }
}
