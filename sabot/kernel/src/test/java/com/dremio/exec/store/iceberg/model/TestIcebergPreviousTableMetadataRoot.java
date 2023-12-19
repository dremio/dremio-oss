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
package com.dremio.exec.store.iceberg.model;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;

/**
 * This test class verifies the IcebergBaseModel.getPreviousMetadataRoot() Method. This test ensures the metadata
 * location matches the location committed to the Nessie Commit Log during physical dataset promotion.
 * Although a user may have relocated the location of the metadata / manifest files, nessie's commit log only has reference to the original path.
 * Metadata path format varies on the distributed storage source type.
 * This test is designed verify support for all distributed storage config sources.
 */
public class TestIcebergPreviousTableMetadataRoot {

  //Constant fields are fake plugin paths intended mimic plugin sources.
  //These are local values hardcoded to mimic server connections.
  //We want to ensure for each config Source, the ABS path turns into the EXPECT path
  final String S3_PATH_ABS = "s3://fake.dummy.authentication/usr/A/metadata/fb395f25-4bd7-4858-bbc0-10654b5ebe58/metadata/00003-e8a08c76-dda8-47d9-b5ce-0705a7f39155.metadata.json";
  final Path S3_PATH_EXPECT = Path.of("/fake.dummy.authentication/usr/A/metadata/fb395f25-4bd7-4858-bbc0-10654b5ebe58");
  final String HDFS_PATH_ABS = "hdfs://fakelocalhost:8020/A/metadata/150ee91d-3db8-4a31-95af-69bacd566dc0/metadata/00008-c707841a-8efe-4736-b303-74dbad99524c.metadata.json";
  final Path HDFS_PATH_EXPECT = Path.of("/A/metadata/150ee91d-3db8-4a31-95af-69bacd566dc0");
  final String AZURE_PATH_ABS = "wasbs://end@firstname.blob.core.windows.net/metadata/a6396692-03a7-4eba-a988-de4b1babe588/metadata/00000-0d1a2fc0-f5de-4695-9f0a-c1b1f717fd83.metadata.json";
  final Path AZURE_PATH_EXPECT = Path.of("/end/metadata/a6396692-03a7-4eba-a988-de4b1babe588");
  final String NAS_PATH_ABS = "file:///Users/firstname.lastname/myNAS/A/metadata/3def2c01-94bc-4089-abb3-12c9488beeee/metadata/00007-8298eb39-73cc-41ae-9030-be8893c08115.metadata.json";
  final Path NAS_PATH_EXPECT = Path.of("/Users/firstname.lastname/myNAS/A/metadata/3def2c01-94bc-4089-abb3-12c9488beeee");

  /**
   * Tests IcebergBaseModel.getPreviousTableMetadataRoot() on NAS distributed storage source config
   */
  @Test
  public void testNAS() {
    Assert.assertEquals(NAS_PATH_EXPECT, testPreviousTableMetadataRoot(NAS_PATH_ABS));
  }

  /**
   * Tests IcebergBaseModel.getPreviousTableMetadataRoot() on a fake S3 distributed storage source config
   * NOTE: test does not actually connect to a S3 source
   */
  @Test
  public void testS3() {
    Assert.assertEquals(S3_PATH_EXPECT, testPreviousTableMetadataRoot(S3_PATH_ABS));
  }

  /**
   * Tests IcebergBaseModel.getPreviousTableMetadataRoot() on HDFS distributed storage source config
   * NOTE: test does not actually connect to a HDFS instance
   */
  @Test
  public void testHDFS() {
    Assert.assertEquals(HDFS_PATH_EXPECT, testPreviousTableMetadataRoot(HDFS_PATH_ABS));
  }

  /**
   * Tests IcebergBaseModel.getPreviousTableMetadataRoot() on Azure distributed storage source config
   * NOTE: test does not actually connect to an Azure container
   */
  @Test
  public void testAzure() {
    Assert.assertEquals(AZURE_PATH_EXPECT, testPreviousTableMetadataRoot(AZURE_PATH_ABS));
  }

  private Path testPreviousTableMetadataRoot(String absolutePath) {
    IcebergMetadata icebergMetadata = new IcebergMetadata();
    icebergMetadata.setMetadataFileLocation(absolutePath);
    return IcebergUtils.getPreviousTableMetadataRoot(icebergMetadata);
  }
}
