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

import com.dremio.connector.impersonation.extensions.SupportsImpersonation;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Provider;

/** HDFS Storage plugin */
public class HDFSStoragePlugin extends FileSystemPlugin<HDFSConf> implements SupportsImpersonation {
  /**
   * HDFS options to enable and use HDFS short-circuit reads. Once MapR profile Hadoop dependency
   * version is upgraded to 2.8.x, use below constants defined in hadoop code:
   * org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Read.ShortCircuit.KEY
   * org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY
   */
  private static final String HDFS_READ_SHORTCIRCUIT_KEY = "dfs.client.read.shortcircuit";

  private static final String HDFS_DFS_DOMAIN_SOCKET_PATH_KEY = "dfs.domain.socket.path";

  public HDFSStoragePlugin(
      HDFSConf config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
    // TODO Auto-generated constructor stub
  }

  @Override
  protected List<Property> getProperties() {
    final List<Property> result = new ArrayList<>();

    List<Property> properties = super.getProperties();
    if (properties != null) {
      result.addAll(properties);
    }

    final HDFSConf hdfsConf = getConfig();
    final HDFSConf.ShortCircuitFlag shortCircuitFlag = hdfsConf.getShortCircuitFlag();
    if (shortCircuitFlag == null || shortCircuitFlag == HDFSConf.ShortCircuitFlag.SYSTEM) {
      return result;
    }

    switch (hdfsConf.getShortCircuitFlag()) {
      case SYSTEM:
        break;
      case DISABLED:
        result.add(new Property(HDFS_READ_SHORTCIRCUIT_KEY, "false"));
        break;
      case ENABLED:
        result.add(new Property(HDFS_READ_SHORTCIRCUIT_KEY, "true"));
        result.add(
            new Property(
                HDFS_DFS_DOMAIN_SOCKET_PATH_KEY,
                Optional.ofNullable(hdfsConf.getShortCircuitSocketPath()).orElse("")));
        break;
      default:
        throw new AssertionError("Unknown enum value: " + hdfsConf.getShortCircuitFlag());
    }

    return result;
  }

  @Override
  public boolean isImpersonationEnabled() {
    return getConfig().isImpersonationEnabled();
  }
}
