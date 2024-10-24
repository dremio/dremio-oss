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

import static com.dremio.common.utils.SqlUtils.quotedCompound;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.ReservedCharacters;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class representing a generic catalog entity key */
@JsonDeserialize(builder = CatalogEntityKey.Builder.class)
public final class CatalogEntityKey {
  public static final String KEY_DELIMITER =
      ReservedCharacters.getInformationSeparatorOne(); // : separated key and version
  private static final Logger logger = LoggerFactory.getLogger(CatalogEntityKey.class);
  private final List<String> keyComponents;
  private final @Nullable TableVersionContext tableVersionContext;
  private final String joinedKeyWithVersion; // see toString()

  private CatalogEntityKey(
      List<String> keyComponents, @Nullable TableVersionContext tableVersionContext) {
    this.keyComponents = ImmutableList.copyOf(keyComponents);
    this.tableVersionContext = tableVersionContext;
    this.joinedKeyWithVersion = toString(this.keyComponents, this.tableVersionContext);
  }

  public static Builder newBuilder() {
    return new CatalogEntityKey.Builder();
  }

  public int size() {
    return keyComponents.size();
  }

  public List<String> getKeyComponents() {
    return keyComponents;
  }

  public @Nullable TableVersionContext getTableVersionContext() {
    return tableVersionContext;
  }

  // TODO(DX-85701) :  This check might need to change if  NOT_SPECIFIED is made the default value
  // for TableVersionContext
  public boolean hasTableVersionContext() {
    return tableVersionContext != null;
  }

  /**
   * @return plain key without version context
   */
  public static CatalogEntityKey of(String... keyComponents) {
    return of(Arrays.asList(keyComponents));
  }

  /**
   * @return plain key without version context
   */
  public static CatalogEntityKey of(List<String> keyComponents) {
    return CatalogEntityKey.newBuilder().keyComponents(keyComponents).build();
  }

  public NamespaceKey toNamespaceKey() {
    return new NamespaceKey(keyComponents);
  }

  public static @Nullable CatalogEntityKey fromNamespaceKey(NamespaceKey namespaceKey) {
    if (namespaceKey == null) {
      return null;
    }
    return CatalogEntityKey.newBuilder()
        .keyComponents(namespaceKey.getPathComponents())
        .tableVersionContext(null)
        .build();
  }

  public static CatalogEntityKey fromNamespaceContainer(NameSpaceContainer nameSpaceContainer) {
    return fromNamespaceKey(new NamespaceKey(nameSpaceContainer.getFullPathList()));
  }

  public static CatalogEntityKey fromString(String keyInStringFormat) {
    List<String> keyParts = Arrays.asList(keyInStringFormat.split(KEY_DELIMITER));
    List<String> keyPaths = PathUtils.parseFullPath(keyParts.get(0));
    if (keyPaths.isEmpty() || keyParts.size() > 2) {
      logger.debug("Invalid CatalogEntityKey format {}", keyInStringFormat);
      throw UserException.validationError()
          .message("Invalid CatalogEntityKey format %s", keyInStringFormat)
          .build(logger);
    }
    TableVersionContext tVersionContext = null;
    if (keyParts.size() == 2) {
      String versionString = keyParts.get(1);
      if (versionString != null) {
        tVersionContext = TableVersionContext.deserialize(versionString);
      }
    }
    return CatalogEntityKey.newBuilder()
        .keyComponents(keyPaths)
        .tableVersionContext(tVersionContext)
        .build();
  }

  private static String toString(
      List<String> keyComponents, TableVersionContext tableVersionContext) {
    StringBuilder keyWithVersion = new StringBuilder();
    String keyWithoutVersion = quotedCompound(keyComponents);
    keyWithVersion.append(keyWithoutVersion);
    if (tableVersionContext != null) {
      keyWithVersion.append(KEY_DELIMITER);
      keyWithVersion.append(tableVersionContext.serialize());
    }
    return keyWithVersion.toString();
  }

  public String toStringWithoutVersionContext() {
    return quotedCompound(keyComponents);
  }

  @JsonIgnore
  public String getLeaf() {
    return keyComponents.get(keyComponents.size() - 1);
  }

  @Override
  public int hashCode() {
    return joinedKeyWithVersion.hashCode();
  }

  public CatalogEntityKey asLowerCase() {
    return CatalogEntityKey.newBuilder()
        .keyComponents(keyComponents.stream().map(String::toLowerCase).collect(Collectors.toList()))
        .tableVersionContext(tableVersionContext)
        .build();
  }

  @JsonIgnore
  public boolean isKeyForImmutableEntity() {
    if (tableVersionContext == null) {
      return false;
    }
    switch (tableVersionContext.getType()) {
      case SNAPSHOT_ID:
      case COMMIT:
      case TIMESTAMP:
        return true;
      default:
        return false;
    }
  }

  @Override
  public String toString() {
    return joinedKeyWithVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CatalogEntityKey that = (CatalogEntityKey) o;
    return Objects.equals(keyComponents, that.keyComponents)
        && Objects.equals(tableVersionContext, that.tableVersionContext);
  }

  @JsonIgnore
  public String getEntityName() {
    return keyComponents.get(keyComponents.size() - 1);
  }

  @JsonIgnore
  public String getRootEntity() {
    return keyComponents.get(0);
  }

  @JsonIgnore
  public List<String> getPathWithoutRoot() {
    return keyComponents.subList(1, keyComponents.size());
  }

  @JsonIgnore
  public CatalogEntityKey getParent() {
    return CatalogEntityKey.newBuilder()
        .keyComponents((keyComponents.subList(0, keyComponents.size() - 1)))
        .tableVersionContext(tableVersionContext)
        .build();
  }

  public String toUnescapedString() {
    return quotedCompound(keyComponents);
  }

  public String toSql() {
    StringBuilder keyWithVersion = new StringBuilder();
    keyWithVersion.append(quotedCompound(keyComponents));

    if (tableVersionContext != null) {
      keyWithVersion.append(" AT ");
      keyWithVersion.append(tableVersionContext.toSql());
    }

    return keyWithVersion.toString();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder {
    private List<String> keyComponents;
    private TableVersionContext tableVersionContext;

    public Builder() {}

    public Builder keyComponents(String... keyComponents) {
      return keyComponents(Arrays.asList(keyComponents));
    }

    @JsonSetter
    public Builder keyComponents(List<String> key) {
      keyComponents = Preconditions.checkNotNull(key);
      return this;
    }

    public Builder tableVersionContext(TableVersionContext versionContext) {
      tableVersionContext = versionContext;
      return this;
    }

    public CatalogEntityKey build() {
      Preconditions.checkNotNull(keyComponents);
      if (keyComponents.stream()
          .anyMatch((a) -> a.contains(ReservedCharacters.getInformationSeparatorOne()))) {
        throw UserException.validationError()
            .message("Invalid CatalogEntityKey format %s", keyComponents)
            .build(logger);
      }
      return new CatalogEntityKey(keyComponents, tableVersionContext);
    }
  }

  public static CatalogEntityKey fromVersionedDatasetId(VersionedDatasetId versionedDatasetId) {
    return CatalogEntityKey.newBuilder()
        .keyComponents(versionedDatasetId.getTableKey())
        .tableVersionContext(versionedDatasetId.getVersionContext())
        .build();
  }

  public static CatalogEntityKey namespaceKeyToCatalogEntityKey(
      NamespaceKey namespaceKey, VersionContext versionContext) {
    return namespaceKeyToCatalogEntityKey(namespaceKey, TableVersionContext.of(versionContext));
  }

  public static CatalogEntityKey namespaceKeyToCatalogEntityKey(
      NamespaceKey namespaceKey, TableVersionContext versionContext) {
    return CatalogEntityKey.newBuilder()
        .keyComponents(namespaceKey.getPathComponents())
        .tableVersionContext(versionContext)
        .build();
  }

  public static CatalogEntityKey buildCatalogEntityKeyDefaultToNotSpecifiedVersionContext(
      NamespaceKey namespaceKey, VersionContext sessionVersion) {
    TableVersionContext tableVersionContext = TableVersionContext.NOT_SPECIFIED;
    if (sessionVersion != null) {
      tableVersionContext = TableVersionContext.of(sessionVersion);
    }

    return CatalogEntityKey.namespaceKeyToCatalogEntityKey(namespaceKey, tableVersionContext);
  }
}
