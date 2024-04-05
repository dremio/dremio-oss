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

package com.dremio.exec.hive;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.Assert.assertEquals;

import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.store.hive.Hive2StoragePluginConfig;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.dremio.exec.store.hive.HiveStoragePluginConfig;
import java.util.ArrayList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

/** Tests for {@link com.dremio.exec.store.hive.HiveConfFactory} */
public class TestHiveConfFactory {

  @Test
  public void testS3ImplDefaults() {
    HiveConfFactory hiveConfFactory = new HiveConfFactory();
    HiveConf confWithDefaults = hiveConfFactory.createHiveConf(getTestConfig());
    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", confWithDefaults.get("fs.s3.impl"));
    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", confWithDefaults.get("fs.s3n.impl"));

    HiveStoragePluginConfig configWithOverrides = getTestConfig();
    configWithOverrides.propertyList = new ArrayList<>();
    configWithOverrides.propertyList.add(
        new Property("fs.s3.impl", "com.dremio.test.CustomS3Impl"));
    configWithOverrides.propertyList.add(
        new Property("fs.s3n.impl", "com.dremio.test.CustomS3NImpl"));
    HiveConf confWithOverrides = hiveConfFactory.createHiveConf(configWithOverrides);
    assertEquals("com.dremio.test.CustomS3Impl", confWithOverrides.get("fs.s3.impl"));
    assertEquals("com.dremio.test.CustomS3NImpl", confWithOverrides.get("fs.s3n.impl"));
  }

  @Test
  public void testS3ImplDefaultsUseSecretPropertyList() {
    HiveConfFactory hiveConfFactory = new HiveConfFactory();
    HiveConf confWithDefaults = hiveConfFactory.createHiveConf(getTestConfig());
    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", confWithDefaults.get("fs.s3.impl"));
    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", confWithDefaults.get("fs.s3n.impl"));

    HiveStoragePluginConfig configWithOverrides = getTestConfig();
    configWithOverrides.secretPropertyList = new ArrayList<>();
    configWithOverrides.secretPropertyList.add(
        new Property("fs.s3.impl", "com.dremio.test.CustomS3Impl"));
    configWithOverrides.secretPropertyList.add(
        new Property("fs.s3n.impl", "com.dremio.test.CustomS3NImpl"));
    HiveConf confWithOverrides = hiveConfFactory.createHiveConf(configWithOverrides);
    assertEquals("com.dremio.test.CustomS3Impl", confWithOverrides.get("fs.s3.impl"));
    assertEquals("com.dremio.test.CustomS3NImpl", confWithOverrides.get("fs.s3n.impl"));
  }

  @Test
  public void testUnsupportedHiveConfigs() {
    HiveConfFactory hiveConfFactory = new HiveConfFactory();
    HiveStoragePluginConfig conf = getTestConfig();
    conf.propertyList = new ArrayList<>();
    conf.propertyList.add(new Property("parquet.column.index.access", "true"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> hiveConfFactory.createHiveConf(conf))
        .withMessageContaining("parquet.column.index.access");
  }

  @Test
  public void testUnsupportedHiveConfigsUseSecretPropertyList() {
    HiveConfFactory hiveConfFactory = new HiveConfFactory();
    HiveStoragePluginConfig conf = getTestConfig();
    conf.secretPropertyList = new ArrayList<>();
    conf.secretPropertyList.add(new Property("parquet.column.index.access", "true"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> hiveConfFactory.createHiveConf(conf))
        .withMessageContaining("parquet.column.index.access");
  }

  private HiveStoragePluginConfig getTestConfig() {
    Hive2StoragePluginConfig hive2StoragePluginConfig = new Hive2StoragePluginConfig();
    hive2StoragePluginConfig.hostname = "localhost";
    return hive2StoragePluginConfig;
  }
}
