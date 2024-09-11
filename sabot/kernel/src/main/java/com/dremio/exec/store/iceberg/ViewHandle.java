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

import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.VersionedDatasetAdapter;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.VersionedDatasetHandle;
import java.util.Objects;

public class ViewHandle implements VersionedDatasetHandle {
  private EntityPath viewPath;
  private IcebergViewMetadata icebergViewMetadata;
  private String id;
  private String uniqueId;

  private ViewHandle(
      final EntityPath viewpath,
      IcebergViewMetadata icebergViewMetadata,
      String id,
      String uniqueId) {
    this.viewPath = viewpath;
    this.icebergViewMetadata = icebergViewMetadata;
    this.id = id;
    this.uniqueId = uniqueId;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public VersionedPlugin.EntityType getType() {
    return VersionedPlugin.EntityType.ICEBERG_VIEW;
  }

  @Override
  public DremioTable translateToDremioTable(VersionedDatasetAdapter vda, String accessUserName) {
    return vda.translateIcebergView(accessUserName);
  }

  @Override
  public String getUniqueInstanceId() {
    return uniqueId;
  }

  public IcebergViewMetadata getIcebergViewMetadata() {
    return icebergViewMetadata;
  }

  @Override
  public String getContentId() {
    return id;
  }

  @Override
  public EntityPath getDatasetPath() {
    return viewPath;
  }

  public static final class Builder {
    private EntityPath viewPath;
    private IcebergViewMetadata icebergViewMetadata;
    String id;
    String uniqueId;

    public Builder datasetpath(EntityPath viewpath) {
      this.viewPath = viewpath;
      return this;
    }

    public Builder icebergViewMetadata(IcebergViewMetadata icebergViewMetadata) {
      this.icebergViewMetadata = icebergViewMetadata;
      return this;
    }

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder uniqueId(String uid) {
      this.uniqueId = uid;
      return this;
    }

    public ViewHandle build() {
      Objects.requireNonNull(viewPath, "dataset path is required");
      return new ViewHandle(viewPath, icebergViewMetadata, id, uniqueId);
    }
  }
}
