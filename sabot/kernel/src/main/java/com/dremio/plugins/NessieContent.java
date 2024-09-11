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
package com.dremio.plugins;

import com.dremio.exec.catalog.VersionedPlugin.EntityType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.UDF;

public final class NessieContent {
  private final List<String> catalogKey;
  private final String contentId;
  private final EntityType entityType;
  private final @Nullable String metadataLocation;
  private final @Nullable String viewDialect;

  public NessieContent(
      List<String> catalogKey,
      String contentId,
      EntityType entityType,
      @Nullable String metadataLocation,
      @Nullable String viewDialect) {
    this.catalogKey = catalogKey;
    this.contentId = contentId;
    this.entityType = entityType;
    this.metadataLocation = metadataLocation;
    this.viewDialect = viewDialect;
  }

  public List<String> getCatalogKey() {
    return catalogKey;
  }

  public String getContentId() {
    return contentId;
  }

  public EntityType getEntityType() {
    return entityType;
  }

  /** only available for iceberg table or views */
  public Optional<String> getMetadataLocation() {
    return Optional.ofNullable(metadataLocation);
  }

  /** only available for some iceberg views */
  public Optional<String> getViewDialect() {
    return Optional.ofNullable(viewDialect);
  }

  public static NessieContent buildFromRawContent(List<String> catalogKey, Content rawContent) {
    return new NessieContent(
        catalogKey,
        rawContent.getId(),
        extractVersionedEntityType(rawContent),
        extractMetadataLocation(rawContent),
        extractViewDialect(rawContent));
  }

  private static EntityType extractVersionedEntityType(Content content) {
    Content.Type contentType = content.getType();
    if (Content.Type.ICEBERG_TABLE.equals(contentType)) {
      return EntityType.ICEBERG_TABLE;
    } else if (Content.Type.ICEBERG_VIEW.equals(contentType)) {
      return EntityType.ICEBERG_VIEW;
    } else if (Content.Type.NAMESPACE.equals(contentType)) {
      return EntityType.FOLDER;
    } else if (Content.Type.UDF.equals(contentType)) {
      return EntityType.UDF;
    } else {
      throw new IllegalStateException("Unsupported contentType: " + contentType);
    }
  }

  private static @Nullable String extractViewDialect(Content content) {
    if (content instanceof IcebergView) {
      return ((IcebergView) content).getDialect();
    }

    return null;
  }

  private static @Nullable String extractMetadataLocation(Content content) {
    if (content instanceof IcebergTable) {
      IcebergTable icebergTable = (IcebergTable) content;
      return icebergTable.getMetadataLocation();
    } else if (content instanceof IcebergView) {
      IcebergView icebergView = (IcebergView) content;
      return icebergView.getMetadataLocation();
    } else if (content instanceof UDF) {
      UDF udf = (UDF) content;
      return udf.getMetadataLocation();
    }

    return null;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        Objects.hash(catalogKey.toArray()), contentId, entityType, metadataLocation, viewDialect);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NessieContent)) {
      return false;
    }
    NessieContent other = (NessieContent) obj;
    if (this.entityType != other.entityType) {
      return false;
    }
    if (!this.contentId.equals(other.contentId)) {
      return false;
    }
    if (!this.catalogKey.equals(other.catalogKey)) {
      return false;
    }
    if ((this.metadataLocation == null) ^ (other.metadataLocation == null)) {
      return false;
    }
    if (this.metadataLocation != null && !this.metadataLocation.equals(other.metadataLocation)) {
      return false;
    }
    if ((this.viewDialect == null) ^ (other.viewDialect == null)) {
      return false;
    }
    if (this.viewDialect != null && !this.viewDialect.equals(other.viewDialect)) {
      return false;
    }
    return true;
  }
}
