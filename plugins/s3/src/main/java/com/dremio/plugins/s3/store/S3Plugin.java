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

import java.net.URLEncoder;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.namespace.SourceState;


/**
 * Storage plugin for Amazon Simple Storage Service (S3).
 *
 * It is a variation of {@link FileSystemPlugin} to allow:
 * <ul>
 *   <li>
 *     Implicit listing of buckets owned by the account with given credentials. Plugin configuration doesn't need
 *     to list these buckets. These buckets are exposed as workspaces under the storage plugin.
 *   </li>
 *   <li>
 *     Add external buckets which the given account has access. Each bucket is exposed as separate workspace under
 *     the storage plugin.
 *   </li>
 * </ul>
 */
public class S3Plugin extends FileSystemPlugin {
  private static final Logger logger = LoggerFactory.getLogger(S3Plugin.class);

  /**
   * Default URI schema for S3 file paths.
   */
  public static final String S3_URI_SCHEMA = "s3a://";

  public S3Plugin(S3PluginConfig config, SabotContext dContext, String storageName)
      throws ExecutionSetupException {
    super(config, dContext, storageName);
  }

  @Override
  public boolean refreshState() {
    try {
      String urlSafeName = URLEncoder.encode(getStorageName(), "UTF-8");
      getFsConf().set(FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3://" + urlSafeName);
      // when/if eventually we decide to refresh on the schedule following code will have to be added
      // dfs.initialize(dfs.getUri(), dfs.getConf());
      // at this point it is quite expensive, since we get region per bucket during init time
      return setState(SourceState.GOOD);
    } catch (Exception e) {
      return setState(badState(e));
    }
  }
}
