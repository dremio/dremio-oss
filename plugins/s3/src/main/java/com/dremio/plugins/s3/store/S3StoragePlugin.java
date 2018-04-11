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
package com.dremio.plugins.s3.store;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;

import javax.inject.Provider;

import org.apache.hadoop.fs.FileSystem;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.plugins.util.ContainerFileSystem.ContainerFailure;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;

/**
 * S3 Extension of FileSystemStoragePlugin
 */
public class S3StoragePlugin extends FileSystemPlugin {

  public S3StoragePlugin(S3PluginConfig config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    super(config, context, name, null, idProvider);
  }

  @Override
  public SourceState getState() {
    try {
      ensureDefaultName();
      S3FileSystem fs = getFS(ImpersonationUtil.getProcessUserName()).unwrap(S3FileSystem.class);
      fs.refreshFileSystems();
      List<ContainerFailure> failures = fs.getSubFailures();
      if(failures.isEmpty()) {
        return SourceState.GOOD;
      }
      StringBuilder sb = new StringBuilder();
      for(ContainerFailure f : failures) {
        sb.append(f.getName());
        sb.append(": ");
        sb.append(f.getException().getMessage());
        sb.append("\n");
      }

      return SourceState.warnState(sb.toString());

    } catch (Exception e) {
      return SourceState.badState(e);
    }
  }

  private void ensureDefaultName() throws IOException {
    String urlSafeName = URLEncoder.encode(getName(), "UTF-8");
    getFsConf().set(FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3://" + urlSafeName);
    final FileSystemWrapper fs = getFS(ImpersonationUtil.getProcessUserName());
    // do not use fs.getURI() or fs.getConf() directly as they will produce wrong results
    fs.initialize(URI.create(getFsConf().get(FileSystem.FS_DEFAULT_NAME_KEY)), getFsConf());
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
    // have to do it to set correct FS_DEFAULT_NAME
    ensureDefaultName();

    return Collections.emptyList(); // file system does not know about physical datasets
  }
}
