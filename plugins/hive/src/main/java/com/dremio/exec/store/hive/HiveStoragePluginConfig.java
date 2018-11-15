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
package com.dremio.exec.store.hive;

import javax.inject.Provider;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;

import io.protostuff.Tag;

@SourceType(value = "HIVE", label = "Hive")
public class HiveStoragePluginConfig extends BaseHiveStoragePluginConfig<HiveStoragePluginConfig, HiveStoragePlugin> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStoragePluginConfig.class);

  /**
   * Type of authorization used for this source
   */
  public enum AuthorizationType {
    @Tag(1) @DisplayMetadata(label = "Storage Based with User Impersonation") STORAGE,
    @Tag(2) @DisplayMetadata(label = "SQL Based") SQL,
  }

  //  Note: Tags 1-5 come from BaseHiveStoragePluginConf
  //  optional string hostname = 1;
  //  optional int32 port = 2 [default = 9083];
  //  optional bool enableSasl = 3 [default = false];
  //  optional string kerberosPrincipal = 4;
  //  repeated Property property = 5;
  //  optional bool authType = 6;

  /*
   * Specify the authorization type.
   */
  @Tag(6)
  @DisplayMetadata(label = "Client")
  public AuthorizationType authType = AuthorizationType.STORAGE;

  public HiveStoragePluginConfig() {
  }

  @Override
  public HiveStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new HiveStoragePlugin(addAuthenticationSettings(createHiveConf(this), this), context, name);
  }

  protected static HiveConf addAuthenticationSettings(HiveConf hiveConf, HiveStoragePluginConfig config) {
    switch(config.authType) {
      case STORAGE:
        // populate hiveConf with default authorization values
        break;
      case SQL:
        // Turn on sql-based authorization
        setConf(hiveConf, ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
        setConf(hiveConf, ConfVars.HIVE_AUTHENTICATOR_MANAGER, "org.apache.hadoop.hive.ql.security.ProxyUserAuthenticator");
        setConf(hiveConf, ConfVars.HIVE_AUTHORIZATION_MANAGER, "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
        setConf(hiveConf, ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
        break;
      default:
        // Code should not reach here
        throw new UnsupportedOperationException("Unknown authorization type " + config.authType);
    }
    return hiveConf;
  }
}
