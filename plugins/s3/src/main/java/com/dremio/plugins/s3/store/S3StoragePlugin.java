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
import java.net.URLEncoder;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;

/**
 * S3 Extension of FileSystemStoragePlugin
 */
public class S3StoragePlugin extends FileSystemPlugin {

  public S3StoragePlugin(S3PluginConfig config, SabotContext context, String name) throws ExecutionSetupException {
    super(config, context, name);
  }

  @Override
  public SourceState getState() {
    try {
      String urlSafeName = URLEncoder.encode(getName(), "UTF-8");
      getFsConf().set(FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3://" + urlSafeName);
      return SourceState.GOOD;
    } catch (Exception e) {
      return SourceState.badState(e);
    }
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
    // have to do it to set correct FS_DEFAULT_NAME
    getState();
    final FileSystemWrapper fs = getFS(ImpersonationUtil.getProcessUserName());
    // do not use fs.getURI() or fs.getConf() directly as they will produce wrong results
    fs.initialize(URI.create(getFsConf().get(FileSystem.FS_DEFAULT_NAME_KEY)), getFsConf());
    return Collections.emptyList(); // file system does not know about physical datasets
  }
}
