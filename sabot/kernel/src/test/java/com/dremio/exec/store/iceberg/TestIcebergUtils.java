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
package com.dremio.exec.store.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
Test class for IcebergUtils.java class
 */
public class TestIcebergUtils {

  @Test
  public void getValidIcebergPathTest() {
    String testUrl = "/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";

    Configuration azureConf = new Configuration();
    azureConf.set("dremio.azure.account", "azurev1databricks2");
    String modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), azureConf, "dremioAzureStorage://");
    Assert.assertEquals("wasbs://testdir@azurev1databricks2.blob.core.windows.net/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    Configuration conf = new Configuration();

    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), conf, "dremioS3");
    Assert.assertEquals("s3://testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), conf, "dremiogcs");
    Assert.assertEquals("gs://testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    Configuration hdfsConf = new Configuration();
    hdfsConf.set("fs.defaultFS", "hdfs://172.25.0.39:8020/");
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), hdfsConf, "hdfs");
    Assert.assertEquals("hdfs://172.25.0.39:8020/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(testUrl), conf, "hdfs");
    Assert.assertEquals("hdfs:///testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro", modifiedFileLocation);

    String urlWithScheme = "wasbs://testdir@azurev1databricks2.blob.core.windows.net/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(urlWithScheme), conf, "dremioAzureStorage://");
    Assert.assertEquals(urlWithScheme, modifiedFileLocation);


    urlWithScheme = "s3://testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(urlWithScheme), conf, "dremioS3");
    Assert.assertEquals(urlWithScheme, modifiedFileLocation);

    urlWithScheme = "hdfs://172.25.0.39:8020/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(urlWithScheme), conf, "hdfs");
    Assert.assertEquals(urlWithScheme, modifiedFileLocation);

    urlWithScheme = "file:/testdir/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041.avro";
    modifiedFileLocation = IcebergUtils.getValidIcebergPath(new Path(urlWithScheme), conf, "file");
    Assert.assertEquals(urlWithScheme, modifiedFileLocation);

  }
}
