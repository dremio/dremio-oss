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
package com.dremio.exec.store.iceberg.nessie;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.store.iceberg.BaseIcebergTest;
import com.dremio.exec.store.iceberg.VersionedUdfMetadata;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.Udf;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.ImmutableUdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfUtil;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;

class TestVersionedUdfOperations extends BaseIcebergTest {
  protected static final UdfSignature SIGNATURE =
      ImmutableUdfSignature.builder()
          .signatureId(UdfUtil.generateUUID())
          .addParameters(
              required(1, "x", Types.DoubleType.get(), "X coordinate"),
              required(2, "y", Types.DoubleType.get()))
          .returnType(Types.DoubleType.get())
          .deterministic(true)
          .build();

  private static final String CREATE_UDF_SQL = "select sqrt(x * x + y * y)";
  private static final String UDF_USER = "test_user";
  private static final String TEST_BRANCH = "test_branch";
  private static final List<String> UDF_KEY = Arrays.asList("udf", "foo", "bar");

  @Test
  public void testCreate() {
    createBranch(TEST_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(TEST_BRANCH, ContentKey.of(UDF_KEY));

    VersionedUdfBuilder udfBuilder =
        new VersionedUdfBuilder(UDF_KEY)
            .withVersionContext(getVersion(TEST_BRANCH))
            .withWarehouseLocation(getWarehouseLocation())
            .withNessieClient(nessieClient)
            .withFileIO(getFileIO())
            .withUserName(UDF_USER)
            .withDefaultNamespace(Namespace.of("udf", "foo"))
            .withSignature(SIGNATURE)
            .withBody(
                VersionedUdfMetadata.SupportedUdfDialects.DREMIOSQL.toString(),
                CREATE_UDF_SQL,
                "dremio comment");

    Udf udf = udfBuilder.create();
    assertThat(udf).isNotNull();

    dropBranch(TEST_BRANCH, getVersion(TEST_BRANCH));
  }

  @Test
  public void testCreateUnsupportedDialect() {
    createBranch(TEST_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(TEST_BRANCH, ContentKey.of(UDF_KEY));

    assertThatThrownBy(
            () ->
                new VersionedUdfBuilder(UDF_KEY)
                    .withVersionContext(getVersion(TEST_BRANCH))
                    .withWarehouseLocation(getWarehouseLocation())
                    .withNessieClient(nessieClient)
                    .withFileIO(getFileIO())
                    .withUserName(UDF_USER)
                    .withDefaultNamespace(Namespace.of("udf", "foo"))
                    .withSignature(SIGNATURE)
                    .withBody("dremio", CREATE_UDF_SQL, "dremio comment"))
        .hasMessageContaining("Unsupported Udf dialect");
  }

  private String getWarehouseLocation() {
    return warehouseLocation;
  }
}
