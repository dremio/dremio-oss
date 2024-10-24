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
package com.dremio.catalog.model;

import static com.dremio.common.utils.ReservedCharacters.getInformationSeparatorOne;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestCatalogEntityKey {

  private static final String MY_SOURCE_0_NAME = "my_source_0";
  private static final String MY_SOURCE_1_NAME = "my_source_1";
  private static final String MY_FOLDER_0_NAME = "my_folder_0";
  private static final String MY_TABLE_0_NAME = "my_table_0";
  private static final String MY_TABLE_1_NAME = "my_table_1";

  private static final List<String> MY_SOURCE_0_PATH = List.of(MY_SOURCE_0_NAME);
  private static final List<String> MY_SOURCE_1_PATH = List.of(MY_SOURCE_1_NAME);
  private static final List<String> MY_SOURCE_0_MY_TABLE_0_PATH =
      List.of(MY_SOURCE_0_NAME, MY_TABLE_0_NAME);
  private static final List<String> MY_SOURCE_0_MY_TABLE_1_PATH =
      List.of(MY_SOURCE_0_NAME, MY_TABLE_1_NAME);
  private static final List<String> MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH =
      List.of(MY_SOURCE_0_NAME, MY_FOLDER_0_NAME, MY_TABLE_0_NAME);
  private static final List<String> MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_1_PATH =
      List.of(MY_SOURCE_0_NAME, MY_FOLDER_0_NAME, MY_TABLE_1_NAME);
  private static final TableVersionContext MY_BRANCH_0_TABLE_VERSION_CONTEXT =
      TableVersionContext.tryParse("BRANCH", "my_branch_0").orElseThrow();
  private static final TableVersionContext MY_BRANCH_1_TABLE_VERSION_CONTEXT =
      TableVersionContext.tryParse("BRANCH", "my_branch_1").orElseThrow();

  private static Stream<Arguments> provideEqualityTests() {
    return Stream.of(
        Arguments.of(
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_PATH).build(),
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_PATH).build()),
        Arguments.of(
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH).build(),
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH).build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()));
  }

  @ParameterizedTest
  @MethodSource("provideEqualityTests")
  public void testEquality(final CatalogEntityKey left, final CatalogEntityKey right) {
    assertEquals(left, right);
  }

  private static Stream<Arguments> provideInequalityTests() {
    return Stream.of(
        Arguments.of(
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_PATH).build(),
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_1_PATH).build()),
        Arguments.of(
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH).build(),
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_MY_TABLE_1_PATH).build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_1_PATH)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_1_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_1_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_1_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_1_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_1_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_1_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_PATH).build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_PATH).build()),
        Arguments.of(
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH).build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH).build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_PATH).build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_PATH).build()),
        Arguments.of(
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH).build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder().keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH).build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_PATH)
                .tableVersionContext(MY_BRANCH_1_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_1_TABLE_VERSION_CONTEXT)
                .build()),
        Arguments.of(
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_0_TABLE_VERSION_CONTEXT)
                .build(),
            CatalogEntityKey.newBuilder()
                .keyComponents(MY_SOURCE_0_MY_FOLDER_0_MY_TABLE_0_PATH)
                .tableVersionContext(MY_BRANCH_1_TABLE_VERSION_CONTEXT)
                .build()));
  }

  @ParameterizedTest
  @MethodSource("provideInequalityTests")
  public void testInequality(final CatalogEntityKey left, final CatalogEntityKey right) {
    assertNotEquals(left, right);
  }

  @Test
  public void serdeTestWithNullTableVersionContext() throws JsonProcessingException {

    List<String> key1 = ImmutableList.of("x", "y", "z");
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder().keyComponents(key1).tableVersionContext(null).build();

    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writeValueAsString(catalogEntityKey);

    assertThat(json).isNotNull();

    CatalogEntityKey keyFromJson = objectMapper.readValue(json, CatalogEntityKey.class);
    assertThat(keyFromJson).isEqualTo(catalogEntityKey);
  }

  @Test
  public void serdeTestWithTableVersionContext() throws JsonProcessingException {

    List<String> key1 = ImmutableList.of("x", "y", "z");
    TableVersionContext tableVersionContext = TableVersionContext.NOT_SPECIFIED;
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(key1)
            .tableVersionContext(tableVersionContext)
            .build();
    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writeValueAsString(catalogEntityKey);
    assertThat(json).isNotNull();
    CatalogEntityKey keyFromJson = objectMapper.readValue(json, CatalogEntityKey.class);
    assertThat(keyFromJson).isEqualTo(catalogEntityKey);
  }

  @Test
  public void toStringTestWithNamespaceKey() {
    List<String> key1 = ImmutableList.of("x", "y", "z");
    NamespaceKey namespaceKey = new NamespaceKey(key1);
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder().keyComponents(key1).tableVersionContext(null).build();
    assertThat(namespaceKey.toString()).isEqualTo(catalogEntityKey.toString());
  }

  @Test
  public void nullKeyComponents() {
    // Test constructing key with invalid table version context
    TableVersionContext tableVersionContext = TableVersionContext.NOT_SPECIFIED;
    assertThatThrownBy(
            () ->
                CatalogEntityKey.newBuilder()
                    .keyComponents((String) null)
                    .tableVersionContext(tableVersionContext)
                    .build())
        .isInstanceOf(NullPointerException.class);
  }

  private static void assertStringRoundTrip(String expectedToString, CatalogEntityKey key) {
    String serializedKey = key.toString();
    assertThat(serializedKey).isEqualTo(expectedToString);
    CatalogEntityKey deserialized = CatalogEntityKey.fromString(serializedKey);
    // note: details of equals are covered in another test
    assertThat(deserialized).isEqualTo(key);
  }

  @Test
  public void testStringRoundTrip() {
    List<String> keyComponents = Arrays.asList("catalog", "schema", "folder", "table");

    // string representation is hardcoded in this test because ReflectionSettingsStore has old
    // values that need to remain readable without information loss.
    // this is also why we dont use CatalogEntityKey.KEY_DELIMITER here.

    assertStringRoundTrip(
        "catalog.schema.folder.table",
        CatalogEntityKey.newBuilder().keyComponents(keyComponents).build());

    assertStringRoundTrip(
        "catalog.schema.folder.table\u001F{\"type\":\"BRANCH\",\"value\":\"testBranch\"}",
        CatalogEntityKey.newBuilder()
            .keyComponents(keyComponents)
            .tableVersionContext(new TableVersionContext(TableVersionType.BRANCH, "testBranch"))
            .build());

    assertStringRoundTrip(
        "catalog.schema.folder.table\u001F{\"type\":\"NOT_SPECIFIED\",\"value\":\"\"}",
        CatalogEntityKey.newBuilder()
            .keyComponents(keyComponents)
            .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
            .build());
  }

  @Test
  public void testSerialize() {
    TableVersionContext tableVersionContext =
        new TableVersionContext(TableVersionType.BRANCH, "testBranch");
    List<String> keyComponents = Arrays.asList("catalog", "schema", "folder", "tname");
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(keyComponents)
            .tableVersionContext(tableVersionContext)
            .build();

    String serializedKey = catalogEntityKey.toString();
    assertThat(serializedKey).contains(CatalogEntityKey.KEY_DELIMITER);
  }

  @Test
  public void testDeserialize() {
    TableVersionContext tableVersionContext =
        new TableVersionContext(TableVersionType.BRANCH, "testBranch");
    List<String> keyComponents = Arrays.asList("catalog", "schema", "folder", "tname");
    String serializedKeyComponents = PathUtils.constructFullPath(keyComponents);
    String serializedTableVersionContext = tableVersionContext.serialize();
    String stringKey =
        new StringBuilder()
            .append(serializedKeyComponents)
            .append(CatalogEntityKey.KEY_DELIMITER)
            .append(serializedTableVersionContext)
            .toString();
    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromString(stringKey);
    assertThat(catalogEntityKey.getKeyComponents()).isEqualTo(keyComponents);
    assertThat(catalogEntityKey.getTableVersionContext()).isEqualTo(tableVersionContext);
  }

  @Test
  public void testDeserializeNegative() {
    TableVersionContext tableVersionContext =
        new TableVersionContext(TableVersionType.BRANCH, "testBranch");
    List<String> keyComponents = Arrays.asList("catalog", "schema", "folder", "tname");
    String serializedKeyComponents = PathUtils.constructFullPath(keyComponents);
    String serializedTableVersionContext = tableVersionContext.serialize();
    String stringKey =
        new StringBuilder()
            .append(serializedKeyComponents)
            .append("badKEYdelimiter")
            .append(serializedTableVersionContext)
            .toString();

    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromString(stringKey);
    assertThat(catalogEntityKey.getKeyComponents()).isNotEqualTo(keyComponents);
  }

  @Test
  public void testKeyWithInvalidName() {
    TableVersionContext tableVersionContext =
        new TableVersionContext(TableVersionType.BRANCH, "testBranch");
    String tablename =
        new StringBuilder()
            .append("tableWithInvalidCharacter")
            .append(getInformationSeparatorOne())
            .toString();
    List<String> keyComponents = Arrays.asList("catalog", "schema", "folder", tablename);
    CatalogEntityKey.Builder catalogEntityKeyBuilder = CatalogEntityKey.newBuilder();
    assertThatThrownBy(
            () ->
                catalogEntityKeyBuilder
                    .keyComponents(keyComponents)
                    .tableVersionContext(tableVersionContext)
                    .build())
        .isInstanceOf(UserException.class);
    assertThatThrownBy(
            () ->
                catalogEntityKeyBuilder
                    .keyComponents(keyComponents)
                    .tableVersionContext(tableVersionContext)
                    .build())
        .hasMessageContaining("Invalid CatalogEntityKey format ");
  }

  @Test
  public void testKeyWithInvalidFolder() {
    TableVersionContext tableVersionContext =
        new TableVersionContext(TableVersionType.BRANCH, "testBranch");
    String foldername =
        new StringBuilder()
            .append("folderWithInvalidCharacter")
            .append(getInformationSeparatorOne())
            .toString();
    List<String> keyComponents = Arrays.asList("catalog", "schema", foldername, "tname");
    CatalogEntityKey.Builder catalogEntityKeyBuilder = CatalogEntityKey.newBuilder();
    assertThatThrownBy(
            () ->
                catalogEntityKeyBuilder
                    .keyComponents(keyComponents)
                    .tableVersionContext(tableVersionContext)
                    .build())
        .isInstanceOf(UserException.class);
    assertThatThrownBy(
            () ->
                catalogEntityKeyBuilder
                    .keyComponents(keyComponents)
                    .tableVersionContext(tableVersionContext)
                    .build())
        .hasMessageContaining("Invalid CatalogEntityKey format ");
  }

  @Test
  public void testKeyForImmutableEntity() {
    List<String> key1 = ImmutableList.of("x", "y", "z");
    TableVersionContext tableVersionContextImmutable =
        new TableVersionContext(TableVersionType.SNAPSHOT_ID, "1234564");
    final CatalogEntityKey catalogEntityKeyImmutable =
        CatalogEntityKey.newBuilder()
            .keyComponents(key1)
            .tableVersionContext(tableVersionContextImmutable)
            .build();
    assertThat(catalogEntityKeyImmutable.isKeyForImmutableEntity()).isTrue();
    TableVersionContext tableVersionContextImmutable2 =
        new TableVersionContext(TableVersionType.TIMESTAMP, 1000L);
    final CatalogEntityKey catalogEntityKeyImmutable2 =
        CatalogEntityKey.newBuilder()
            .keyComponents(key1)
            .tableVersionContext(tableVersionContextImmutable2)
            .build();
    assertThat(catalogEntityKeyImmutable2.isKeyForImmutableEntity()).isTrue();
    TableVersionContext tableVersionContextImmutable3 =
        new TableVersionContext(TableVersionType.COMMIT, "1234564");
    final CatalogEntityKey catalogEntityKeyImmutable3 =
        CatalogEntityKey.newBuilder()
            .keyComponents(key1)
            .tableVersionContext(tableVersionContextImmutable3)
            .build();
    assertThat(catalogEntityKeyImmutable3.isKeyForImmutableEntity()).isTrue();

    TableVersionContext tableVersionContext1 =
        new TableVersionContext(TableVersionType.BRANCH, "foo");
    final CatalogEntityKey catalogEntityKey1 =
        CatalogEntityKey.newBuilder()
            .keyComponents(key1)
            .tableVersionContext(tableVersionContext1)
            .build();
    assertThat(catalogEntityKey1.isKeyForImmutableEntity()).isFalse();

    TableVersionContext tableVersionContext2 = new TableVersionContext(TableVersionType.TAG, "foo");
    final CatalogEntityKey catalogEntityKey2 =
        CatalogEntityKey.newBuilder()
            .keyComponents(key1)
            .tableVersionContext(tableVersionContext2)
            .build();
    assertThat(catalogEntityKey2.isKeyForImmutableEntity()).isFalse();
  }

  @Test
  public void testBuildCatalogEntityKey() {
    NamespaceKey namespaceKey = new NamespaceKey(Arrays.asList("source", "table"));
    VersionContext versionContext = VersionContext.ofBranch("main");
    CatalogEntityKey actualCatalogEntityKey =
        CatalogEntityKey.buildCatalogEntityKeyDefaultToNotSpecifiedVersionContext(
            namespaceKey, versionContext);
    CatalogEntityKey expectedCatalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(namespaceKey.getPathComponents())
            .tableVersionContext(TableVersionContext.of(versionContext))
            .build();
    assertThat(actualCatalogEntityKey).isEqualTo(expectedCatalogEntityKey);

    actualCatalogEntityKey =
        CatalogEntityKey.buildCatalogEntityKeyDefaultToNotSpecifiedVersionContext(
            namespaceKey, null);
    expectedCatalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(namespaceKey.getPathComponents())
            .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
            .build();
    assertThat(actualCatalogEntityKey).isEqualTo(expectedCatalogEntityKey);

    actualCatalogEntityKey =
        CatalogEntityKey.buildCatalogEntityKeyDefaultToNotSpecifiedVersionContext(
            namespaceKey, VersionContext.NOT_SPECIFIED);
    assertThat(actualCatalogEntityKey).isEqualTo(expectedCatalogEntityKey);
  }
}
