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
package com.dremio.dac.sources;

import static com.dremio.dac.server.NASSourceConfigurator.getDefaultFormats;
import static com.dremio.service.namespace.source.proto.SourceType.HDFS;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.HdfsConfig;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.SchemaMutability;

/**
 * generates a StoragePluginConfig from an HDFS Source
 */
public class HDFSSourceConfigurator extends SingleSourceToStoragePluginConfig<HdfsConfig> {

  public HDFSSourceConfigurator() {
    super(HDFS);
  }

  @Override
  public StoragePluginConfig configureSingle(HdfsConfig hdfs) {
    int port = hdfs.getPort() == null ? 9000 : hdfs.getPort();
    // TODO: validate host
    String hostname = checkNotNull(hdfs.getHostname(), "missing hostname");
    String connection = "hdfs://" + hostname + ":" + port;
    FileSystemConfig config =
        new FileSystemConfig(connection, null, null, getDefaultFormats(), hdfs.getEnableImpersonation(), SchemaMutability.NONE);
    return config;
  }

}
