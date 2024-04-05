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
package com.dremio.exec.catalog;

import static com.dremio.common.utils.ReservedCharacters.getInformationSeparatorOne;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.dremio.catalog.model.CatalogEntityKey;
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
import org.junit.jupiter.api.Test;

public class TestCatalogEntityKey {

  @Test
  public void serdeTestWithNullTableVersionContext() throws JsonProcessingException {

    List<String> key1 = ImmutableList.of("x", "y", "z");
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder().keyComponents(key1).tableVersionContext(null).build();

    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writeValueAsString(catalogEntityKey);

    assertThat(json).isNotNull();

    CatalogEntityKey keyFromJson = objectMapper.readValue(json, CatalogEntityKey.class);
    assertThat(keyFromJson.equals(catalogEntityKey)).isTrue();
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
    assertThat(keyFromJson.equals(catalogEntityKey)).isTrue();
  }

  @Test
  public void toStringTestWithNamespaceKey() {
    List<String> key1 = ImmutableList.of("x", "y", "z");
    NamespaceKey namespaceKey = new NamespaceKey(key1);
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder().keyComponents(key1).tableVersionContext(null).build();
    assertThat(namespaceKey.toString().equals(catalogEntityKey.toString())).isTrue();
  }

  @Test
  public void nullKeyComponents() {
    // Test constructing key with invalid table version context
    TableVersionContext tableVersionContext = TableVersionContext.NOT_SPECIFIED;
    assertThatThrownBy(
            () ->
                CatalogEntityKey.newBuilder()
                    .keyComponents(null)
                    .tableVersionContext(tableVersionContext)
                    .build())
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testSerialize() {
    String serializedKey = null;
    TableVersionContext tableVersionContext =
        new TableVersionContext(TableVersionType.BRANCH, "testBranch");
    List<String> keyComponents = Arrays.asList("catalog", "schema", "folder", "tname");
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(keyComponents)
            .tableVersionContext(tableVersionContext)
            .build();

    serializedKey = catalogEntityKey.toString();
    assertThat(serializedKey.contains(CatalogEntityKey.KEY_DELIMITER)).isTrue();
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
    CatalogEntityKey catalogEntityKey = new CatalogEntityKey(stringKey);
    assertThat(catalogEntityKey.getKeyComponents()).isEqualTo(keyComponents);
    assertThat(catalogEntityKey.getTableVersionContext()).isEqualTo(tableVersionContext);
  }

  @Test
  public void testDeserializeNegative() {
    TableVersionContext tableVersionContext =
        new TableVersionContext(TableVersionType.BRANCH, "testBranch");
    String BAD_KEY_DELIMITER = new String("xyxy");
    List<String> keyComponents = Arrays.asList("catalog", "schema", "folder", "tname");
    String serializedKeyComponents = PathUtils.constructFullPath(keyComponents);
    String serializedTableVersionContext = tableVersionContext.serialize();
    String stringKey =
        new StringBuilder()
            .append(serializedKeyComponents)
            .append(BAD_KEY_DELIMITER)
            .append(serializedTableVersionContext)
            .toString();

    CatalogEntityKey catalogEntityKey = new CatalogEntityKey(stringKey);
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
}
