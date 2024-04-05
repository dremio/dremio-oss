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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class representing a generic catalog entity key */
@JsonDeserialize(builder = CatalogEntityKey.Builder.class)
public class CatalogEntityKey {
  public static final String KEY_DELIMITER =
      ReservedCharacters.getInformationSeparatorOne(); // : separated key and version
  private static final Logger logger = LoggerFactory.getLogger(CatalogEntityKey.class);
  private List<String> keyComponents;
  private TableVersionContext tableVersionContext;
  private String joinedKeyWithVersion; // see toString()

  public CatalogEntityKey(String keyInStringFormat) {
    this(newBuilder().deserialize(keyInStringFormat).build());
  }

  private CatalogEntityKey(Builder builder) {
    this.keyComponents = builder.keyComponents;
    this.tableVersionContext = builder.tableVersionContext;
    this.joinedKeyWithVersion = builder.joinedKeyWithVersion;
  }

  private CatalogEntityKey(CatalogEntityKey catalogEntityKey) {
    this.keyComponents = catalogEntityKey.keyComponents;
    this.tableVersionContext = catalogEntityKey.tableVersionContext;
    this.joinedKeyWithVersion = catalogEntityKey.joinedKeyWithVersion;
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

  public TableVersionContext getTableVersionContext() {
    return tableVersionContext;
  }

  // TODO(DX-85701) :  This check might need to change if  NOT_SPECIFIED is made the default value
  // for TableVersionContext
  public boolean hasTableVersionContext() {
    return tableVersionContext != null;
  }

  public NamespaceKey toNamespaceKey() {
    return new NamespaceKey(keyComponents);
  }

  public static CatalogEntityKey fromNamespaceKey(NamespaceKey namespaceKey) {
    if (namespaceKey == null) {
      return null;
    }
    return CatalogEntityKey.newBuilder()
        .keyComponents(namespaceKey.getPathComponents())
        .tableVersionContext(null)
        .build();
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

  public String toUrlEncodedString() {
    return PathUtils.encodeURIComponent(toString());
  }

  @Override
  public boolean equals(Object obj) {
    boolean pathComponentsComparison = false;
    boolean tableVersionComparison = false;
    if (obj != null && obj instanceof CatalogEntityKey) {
      CatalogEntityKey o = (CatalogEntityKey) obj;
      pathComponentsComparison = keyComponents.equals(o.keyComponents);
      tableVersionComparison =
          tableVersionContext == null ? true : tableVersionContext.equals(o.tableVersionContext);
      return pathComponentsComparison && tableVersionComparison;
    }
    return false;
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

  public String toUnescapedString() {
    return quotedCompound(keyComponents);
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder {
    private List<String> keyComponents;
    private TableVersionContext tableVersionContext;
    private String joinedKeyWithVersion; // see toString()

    public Builder() {}

    public Builder keyComponents(List<String> key) {
      keyComponents = key;
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
      this.joinedKeyWithVersion = serialize();
      return new CatalogEntityKey(this);
    }

    private String serialize() {
      StringBuilder keyWithVersion = new StringBuilder();
      String keyWithoutVersion = quotedCompound(keyComponents);
      String versionString = tableVersionContext == null ? null : tableVersionContext.serialize();
      keyWithVersion.append(keyWithoutVersion);
      if (tableVersionContext != null) {
        keyWithVersion.append(KEY_DELIMITER);
        keyWithVersion.append(versionString);
      }
      return keyWithVersion.toString();
    }

    @JsonIgnore
    public Builder deserialize(String keyInStringFormat) {
      List<String> keyParts = Arrays.asList(keyInStringFormat.split(KEY_DELIMITER));
      TableVersionContext tVersionContext = null;
      List<String> keyPaths = PathUtils.parseFullPath(keyParts.get(0));
      if (keyPaths.isEmpty() || keyParts.size() > 2) {
        logger.debug("Invalid CatalogEntityKey format {}", keyInStringFormat);
        throw UserException.validationError()
            .message("Invalid CatalogEntityKey format %s", keyInStringFormat)
            .build(logger);
      }
      if (keyParts.size() == 2) {
        String versionString = keyParts.get(1);
        tVersionContext =
            versionString == null ? null : TableVersionContext.deserialize(versionString);
      }
      return CatalogEntityKey.newBuilder()
          .keyComponents(keyPaths)
          .tableVersionContext(tVersionContext);
    }
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
}
