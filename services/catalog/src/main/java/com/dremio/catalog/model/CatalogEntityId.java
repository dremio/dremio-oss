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

import java.util.Optional;

public final class CatalogEntityId {
  private final String namespaceEntityId;
  private final VersionedDatasetId versionedDatasetId;

  private CatalogEntityId(String namespaceEntityId) {
    this.namespaceEntityId = namespaceEntityId;
    this.versionedDatasetId = null;
  }

  private CatalogEntityId(VersionedDatasetId versionedDatasetId) {
    this.namespaceEntityId = null;
    this.versionedDatasetId = versionedDatasetId;
  }

  public static CatalogEntityId fromString(String stringInput) {
    VersionedDatasetId versionedDatasetId = VersionedDatasetId.tryParse(stringInput);
    if (versionedDatasetId != null) {
      return new CatalogEntityId(versionedDatasetId);
    } else {
      return new CatalogEntityId(stringInput);
    }
  }

  public static CatalogEntityId fromVersionedDatasetId(VersionedDatasetId versionedDatasetId) {
    return new CatalogEntityId(versionedDatasetId);
  }

  public Optional<String> toNamespaceEntityId() {
    return Optional.ofNullable(namespaceEntityId);
  }

  public Optional<VersionedDatasetId> toVersionedDatasetId() {
    return Optional.ofNullable(versionedDatasetId);
  }

  @Override
  public String toString() {
    if (namespaceEntityId != null) {
      return namespaceEntityId;
    }
    if (versionedDatasetId != null) {
      return versionedDatasetId.asString();
    }
    return "";
  }

  @Override
  public final int hashCode() {
    // TODO - Does this actually make the hashcode unique?
    if (namespaceEntityId == null && versionedDatasetId != null) {
      return versionedDatasetId.hashCode();
    }
    if (namespaceEntityId != null && versionedDatasetId == null) {
      return namespaceEntityId.hashCode();
    }
    return 0;
  }

  @Override
  public final boolean equals(final Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof CatalogEntityId)) {
      return false;
    }
    CatalogEntityId otherId = (CatalogEntityId) other;
    if (otherId.namespaceEntityId != null) {
      return otherId.namespaceEntityId.equals(namespaceEntityId);
    }
    if (otherId.versionedDatasetId != null) {
      return otherId.versionedDatasetId.equals(versionedDatasetId);
    }
    return false;
  }
}
