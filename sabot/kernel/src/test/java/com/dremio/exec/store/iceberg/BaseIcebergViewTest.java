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

import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedViewsV0;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedViewsV1;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadataParser;
import org.apache.iceberg.view.ViewMetadataParser;
import org.junit.jupiter.api.BeforeAll;

public class BaseIcebergViewTest extends BaseIcebergTest {
  protected static IcebergNessieVersionedViewsV0 icebergNessieVersionedViewsV0;
  protected static IcebergNessieVersionedViewsV1 icebergNessieVersionedViewsV1;

  @BeforeAll
  public static void setup() throws Exception {
    BaseIcebergTest.setup();

    icebergNessieVersionedViewsV0 =
        new IcebergNessieVersionedViewsV0(
            warehouseLocation,
            nessieClient,
            fileIO,
            userName,
            BaseIcebergViewTest::viewVersionMetadataLoader);
    icebergNessieVersionedViewsV1 =
        new IcebergNessieVersionedViewsV1(
            warehouseLocation,
            nessieClient,
            fileIO,
            userName,
            BaseIcebergViewTest::viewMetadataLoader);
  }

  protected static IcebergViewMetadata viewVersionMetadataLoader(String metadataLocation) {
    return IcebergViewMetadataImplV0.of(
        ViewVersionMetadataParser.read(fileIO.newInputFile(metadataLocation)), metadataLocation);
  }

  protected static IcebergViewMetadata viewMetadataLoader(String metadataLocation) {
    return IcebergViewMetadataImplV1.of(
        ViewMetadataParser.read(fileIO.newInputFile(metadataLocation)));
  }
}
