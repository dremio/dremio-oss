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
package com.dremio.exec.store.hbase;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.hibernate.validator.constraints.NotEmpty;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

@SourceType(value = "HBASE", label = "HBase", listable = false)
public class HBaseConf extends ConnectionConf<HBaseConf, HBaseStoragePlugin> {

  private transient Configuration hbaseConf;

  //  optional string zk_quorum = 1;
  //  optional int32 port = 2;
  //  optional bool isSizeCalcEnabled = 3; // enabling region size calculation stats
  //  repeated Property property = 4;

  @NotEmpty
  @Tag(1)
  @DisplayMetadata(label = "Zookeeper Quorum")
  public String zkQuorum;

  @Min(1)
  @Max(65535)
  @Tag(2)
  @DisplayMetadata(label = "Port")
  public int port = 2181;

  /**
   * Enable region size calculation stats.
   */
  @Tag(3)
  @DisplayMetadata(label = "Region size calculation")
  public boolean isSizeCalcEnabled = true;

  @Tag(4)
  public List<Property> propertyList = new ArrayList<>();

  @Override
  public HBaseStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new HBaseStoragePlugin(this, context, name);
  }

  public Configuration getHBaseConf() {
    if (hbaseConf == null) {
      hbaseConf = HBaseConfiguration.create();

      hbaseConf.set("hbase.zookeeper.quorum", zkQuorum);
      hbaseConf.set("hbase.zookeeper.property.clientPort", Integer.toString(port));

      if (propertyList != null) {
        for (Property p : propertyList) {
          hbaseConf.set(p.name, p.value);
        }
      }
    }
    return hbaseConf;
  }


  public String getZookeeperQuorum() {
    return getHBaseConf().get(HConstants.ZOOKEEPER_QUORUM);
  }

  public String getZookeeperport() {
    return getHBaseConf().get(HBaseConstants.HBASE_ZOOKEEPER_PORT);
  }

  @VisibleForTesting
  public void setZookeeperPort(int zookeeperPort) {
    this.propertyList.add(new Property(HBaseConstants.HBASE_ZOOKEEPER_PORT, String.valueOf(zookeeperPort)));
    getHBaseConf().setInt(HBaseConstants.HBASE_ZOOKEEPER_PORT, zookeeperPort);
  }

  @VisibleForTesting
  public void setZookeeper(String zookeeper) {
    this.propertyList.add(new Property(HBaseConstants.HBASE_ZOOKEEPER_QUORUM, zookeeper));
    getHBaseConf().set(HBaseConstants.HBASE_ZOOKEEPER_QUORUM, zookeeper);
  }

}
