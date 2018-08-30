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

import java.util.List;

import javax.inject.Provider;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;

import io.protostuff.Tag;

@SourceType(value = "HIVE", label = "Hive")
public class HiveStoragePluginConfig extends ConnectionConf<HiveStoragePluginConfig, HiveStoragePlugin> {

  //  optional string hostname = 1;
  //  optional int32 port = 2 [default = 9083];
  //  optional bool enableSasl = 3 [default = false];
  //  optional string kerberosPrincipal = 4;
  //  repeated Property property = 5;

  /*
   * Hostname where Hive metastore server is running
   */
  @Tag(1)
  @DisplayMetadata(label = "Hive Metastore Host")
  public String hostname;

  /*
   * Listening port of Hive metastore server
   */
  @Tag(2)
  @DisplayMetadata(label = "Port")
  public int port = 9083;

  /*
   * Is kerberos authentication enabled on metastore services?
   */
  @Tag(3)
  @DisplayMetadata(label = "Enable SASL")
  public boolean enableSasl = false;

  /*
   * Kerberos principal name of metastore servers if kerberos authentication is enabled
   */
  @Tag(4)
  @DisplayMetadata(label = "Hive Kerberos Principal")
  public String kerberosPrincipal;

  /*
   * List of configuration properties.
   */
  @Tag(5)
  public List<Property> propertyList;

  public HiveStoragePluginConfig() {
  }

  @Override
  public HiveStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new HiveStoragePlugin(this, context, name);
  }


}
