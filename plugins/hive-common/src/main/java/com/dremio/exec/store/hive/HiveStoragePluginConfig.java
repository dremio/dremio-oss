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

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.store.StoragePlugin;
import io.protostuff.Tag;

/** Configuration for the Hive storage plugin */
public abstract class HiveStoragePluginConfig
    extends BaseHiveStoragePluginConfig<HiveStoragePluginConfig, StoragePlugin> {
  /** Type of authorization used for this source */
  public enum AuthorizationType {
    @Tag(1)
    @DisplayMetadata(label = "Storage Based with User Impersonation")
    STORAGE,
    @Tag(2)
    @DisplayMetadata(label = "SQL Based")
    SQL,
  }

  //  Note: the below come from BaseHiveStoragePluginConf
  //  optional string hostname = 1;
  //  optional int32 port = 2 [default = 9083];
  //  optional bool enableSasl = 3 [default = false];
  //  optional string kerberosPrincipal = 4;
  //  repeated Property property = 5;
  //  optional bool enableAsync = 11;
  //  optional bool isCachingEnabledForS3AndAzureStorage = 12;
  //  optional bool isCachingEnabledForHDFS = 13;
  //  optional int32 maxCacheSpacePct = 14;
  //  optional DefaultCtasFormatSelection defaultCtasFormat = 15;
  //  hiveMajorVersion = 16;
  //  List<String> secretPropertyList = 20;
  //  List<String> allowedDatabases = 21;
  //
  //  Note: the below come from this file
  //  optional bool authType = 6;

  /*
   * Specify the authorization type.
   */
  @Tag(6)
  @DisplayMetadata(label = "Client")
  public AuthorizationType authType = AuthorizationType.STORAGE;
}
