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
package com.dremio.exec.store;

import static com.dremio.exec.store.SupportsExternalQuery.getExternalQueryFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.tablefunctions.ExternalQuery;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.junit.Test;

/** Test SupportsExternalQuery default methods. */
public class TestSupportsExternalQuery {
  private static final java.util.function.Function<ExternalQuery.ExternalQueryRequest, BatchSchema>
      DUMMY_SCHEMA_BUILDER = (dummy) -> null;
  private static final java.util.function.Function<BatchSchema, RelDataType>
      DUMMY_ROW_TYPE_BUILDER = (dummy) -> null;
  private static final StoragePluginId DUMMY_ID = mock(StoragePluginId.class);

  private Optional<Function> getExternalQueryFunctionHelper(List<String> paths) {
    return getExternalQueryFunction(
        DUMMY_SCHEMA_BUILDER, DUMMY_ROW_TYPE_BUILDER, DUMMY_ID, paths, "");
  }

  @Test
  public void testGetExternalQueryFunc() {
    final List<String> paths = ImmutableList.of("source", "external_query");
    final Optional<Function> func = getExternalQueryFunctionHelper(paths);
    assertTrue(func.isPresent());
    assertTrue(func.get() instanceof ExternalQuery);
  }

  @Test
  public void testGetExternalQueryFuncUpperCase() {
    final List<String> paths = ImmutableList.of("source", "EXTERNAL_QUERY");
    final Optional<Function> func = getExternalQueryFunctionHelper(paths);
    assertTrue(func.isPresent());
    assertTrue(func.get() instanceof ExternalQuery);
  }

  @Test
  public void testGetExternalQueryFuncMixedCase() {
    final List<String> paths = ImmutableList.of("source", "ExtERnal_QuerY");
    final Optional<Function> func = getExternalQueryFunctionHelper(paths);
    assertTrue(func.isPresent());
    assertTrue(func.get() instanceof ExternalQuery);
  }

  @Test
  public void testGetExternalQueryFuncWithBadSyntax() {
    final List<String> paths = ImmutableList.of("source", "externalquery");
    final Optional<Function> func = getExternalQueryFunctionHelper(paths);
    assertFalse(func.isPresent());
  }

  @Test
  public void testGetExternalQueryFuncWithNoSourceName() {
    // External query call without the source name is not allowed.
    final List<String> paths = ImmutableList.of("external_query");
    final Optional<Function> func = getExternalQueryFunctionHelper(paths);
    assertFalse(func.isPresent());
  }

  @Test
  public void testGetExternalQueryFuncWithSourceAndSchema() {
    // Specifying the schema on top of source name is not allowed for external query
    final List<String> paths = ImmutableList.of("source", "schema", "external_query");
    final Optional<Function> func = getExternalQueryFunctionHelper(paths);
    assertFalse(func.isPresent());
  }

  @Test
  public void testGetExternalQueryFuncWithSourceSchemaAndTable() {
    // Specifying the schema and table on top of source name is not allowed for external query
    final List<String> paths = ImmutableList.of("source", "schema", "table", "external_query");
    final Optional<Function> func = getExternalQueryFunctionHelper(paths);
    assertFalse(func.isPresent());
  }

  @Test
  public void testGetExternalQueryFuncWithoutSchemaBuilder() {
    final List<String> paths = ImmutableList.of("source", "external_query");
    assertThatThrownBy(
            () -> getExternalQueryFunction(null, DUMMY_ROW_TYPE_BUILDER, DUMMY_ID, paths, null))
        .hasMessageContaining("schemaBuilder cannot be null.")
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testGetExternalQueryFuncWithoutRowTypeBuilder() {
    final List<String> paths = ImmutableList.of("source", "external_query");
    assertThatThrownBy(
            () -> getExternalQueryFunction(DUMMY_SCHEMA_BUILDER, null, DUMMY_ID, paths, null))
        .hasMessageContaining("rowTypeBuilder cannot be null.")
        .isInstanceOf(NullPointerException.class);
  }
}
