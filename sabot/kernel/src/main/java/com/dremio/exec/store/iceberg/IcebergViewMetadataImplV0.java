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

import com.dremio.exec.store.iceberg.viewdepoc.DremioViewVersionMetadataParser;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadata;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadataParser;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.util.JsonUtil;

public class IcebergViewMetadataImplV0 implements IcebergViewMetadata {
  private ViewVersionMetadata viewMetadataV0;
  private final String metadataLocation;

  private IcebergViewMetadataImplV0(
      final ViewVersionMetadata viewMetadataV0, String metadataLocation) {
    this.viewMetadataV0 = viewMetadataV0;
    this.metadataLocation = metadataLocation;
  }

  public static IcebergViewMetadata of(
      final ViewVersionMetadata viewMetadataV0, String metadataLocation) {
    return new IcebergViewMetadataImplV0(viewMetadataV0, metadataLocation);
  }

  public static ViewVersionMetadata getViewVersionMetadata(
      final IcebergViewMetadata icebergViewMetadata) {
    Preconditions.checkState(
        icebergViewMetadata.getFormatVersion() == SupportedIcebergViewSpecVersion.V0);
    return ((IcebergViewMetadataImplV0) icebergViewMetadata).viewMetadataV0;
  }

  @Override
  public SupportedIcebergViewSpecVersion getFormatVersion() {
    return SupportedIcebergViewSpecVersion.V0;
  }

  @Override
  public Schema getSchema() {
    return viewMetadataV0.definition().schema();
  }

  @Override
  public String getSql() {
    return viewMetadataV0.definition().sql();
  }

  @Override
  public List<String> getSchemaPath() {
    return viewMetadataV0.definition().sessionNamespace();
  }

  @Override
  public String getLocation() {
    return viewMetadataV0.location();
  }

  @Override
  public String getMetadataLocation() {
    return metadataLocation;
  }

  @Override
  public String getUniqueId() {
    return getUUIDFromMetadataLocation(metadataLocation);
  }

  @Override
  public Map<String, String> getProperties() {
    return viewMetadataV0.properties();
  }

  @Override
  public long getCreatedAt() {
    return viewMetadataV0.history().get(0).timestampMillis();
  }

  @Override
  public long getLastModifiedAt() {
    return viewMetadataV0.currentVersion().timestampMillis();
  }

  @Override
  public String getDialect() {
    return SupportedViewDialectsForRead.DREMIO.toString();
  }

  @Override
  public String toJson() {
    return JsonUtil.generate(gen -> ViewVersionMetadataParser.toJson(viewMetadataV0, gen), true);
  }

  @Override
  public IcebergViewMetadata fromJson(String metadataLocation, String json) {
    viewMetadataV0 = DremioViewVersionMetadataParser.fromJson(json);
    return IcebergViewMetadataImplV0.of(viewMetadataV0, metadataLocation);
  }

  private String getUUIDFromMetadataLocation(String metadataLocation) {
    return metadataLocation.substring(
        metadataLocation.lastIndexOf("/") + 1, metadataLocation.lastIndexOf(".metadata.json"));
  }
}
