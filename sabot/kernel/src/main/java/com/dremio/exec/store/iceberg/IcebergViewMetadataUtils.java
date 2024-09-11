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

import com.dremio.exec.store.iceberg.IcebergViewMetadata.SupportedIcebergViewSpecVersion;
import com.dremio.exec.store.iceberg.viewdepoc.BaseVersion;
import com.dremio.exec.store.iceberg.viewdepoc.DremioViewVersionMetadataParser;
import com.dremio.exec.store.iceberg.viewdepoc.HistoryEntry;
import com.dremio.exec.store.iceberg.viewdepoc.Version;
import com.dremio.exec.store.iceberg.viewdepoc.VersionLogEntry;
import com.dremio.exec.store.iceberg.viewdepoc.VersionSummary;
import com.dremio.exec.store.iceberg.viewdepoc.ViewDefinition;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;

/**
 * Utility class specific to
 *
 * @see IcebergViewMetadata interface
 */
public class IcebergViewMetadataUtils {

  public static IcebergViewMetadata fromJson(String metadataLocation, String jsonString) {
    IcebergViewMetadata.SupportedIcebergViewSpecVersion specVersion =
        IcebergUtils.findIcebergViewVersion(jsonString);
    switch (specVersion) {
      case V0:
        return IcebergViewMetadataImplV0.of(
            DremioViewVersionMetadataParser.fromJson(jsonString), metadataLocation);
      case V1:
        return IcebergViewMetadataImplV1.of(ViewMetadataParser.fromJson(jsonString));
      case UNKNOWN:
        throw new RuntimeIOException("Unsupported view metadata json %s", jsonString);
    }
    return null;
  }

  public static String toJson(IcebergViewMetadata viewMetadata) {
    return viewMetadata.toJson();
  }

  public static IcebergViewMetadata translateVersion(
      IcebergViewMetadata icebergViewMetadata, SupportedIcebergViewSpecVersion targetVersion) {
    if (icebergViewMetadata.getFormatVersion() == SupportedIcebergViewSpecVersion.V0
        && targetVersion == SupportedIcebergViewSpecVersion.V1) {
      ViewVersionMetadata viewVersionMetadata =
          IcebergViewMetadataImplV0.getViewVersionMetadata(icebergViewMetadata);
      List<ViewRepresentation> representations = new ArrayList<>();
      representations.add(
          ImmutableSQLViewRepresentation.builder()
              .dialect(IcebergViewMetadata.SupportedViewDialectsForRead.DREMIO.toString())
              .sql(viewVersionMetadata.definition().sql())
              .build());

      ViewMetadata.Builder viewMetadataBuilder =
          ViewMetadata.builder()
              .setProperties(viewVersionMetadata.properties())
              .setLocation(viewVersionMetadata.location());

      viewVersionMetadata.versions().stream()
          .forEach(
              version -> {
                ViewVersion viewVersion =
                    ImmutableViewVersion.builder()
                        // VersionId for creating always defaults to 1 and monotonically increases.
                        .versionId(version.versionId())
                        .schemaId(version.viewDefinition().schema().schemaId())
                        .addAllRepresentations(representations)
                        .defaultNamespace(
                            Namespace.of(
                                version
                                    .viewDefinition()
                                    .sessionNamespace()
                                    .toArray(
                                        new String
                                            [version.viewDefinition().sessionNamespace().size()])))
                        .defaultCatalog(version.viewDefinition().sessionCatalog())
                        .timestampMillis(version.timestampMillis())
                        .putAllSummary(version.summary().properties())
                        .build();
                viewMetadataBuilder.setCurrentVersion(
                    viewVersion, version.viewDefinition().schema());
              });
      ViewMetadata viewMetadata = viewMetadataBuilder.build();
      return IcebergViewMetadataImplV1.of(viewMetadata);
    } else if (icebergViewMetadata.getFormatVersion() == SupportedIcebergViewSpecVersion.V1
        && targetVersion == SupportedIcebergViewSpecVersion.V0) {
      ViewMetadata viewMetadata = IcebergViewMetadataImplV1.getViewMetadata(icebergViewMetadata);
      ViewDefinition viewDefinition =
          ViewDefinition.of(
              icebergViewMetadata.getSql(),
              viewMetadata.schema(),
              viewMetadata.currentVersion().defaultCatalog(),
              Arrays.asList(viewMetadata.currentVersion().defaultNamespace().levels()));
      List<HistoryEntry> historyEntryList =
          viewMetadata.history().stream()
              .map(
                  viewHistoryEntry ->
                      new VersionLogEntry(
                          viewHistoryEntry.timestampMillis(), viewHistoryEntry.versionId()))
              .collect(Collectors.toList());
      List<Version> versions =
          viewMetadata.versions().stream()
              .map(
                  viewVersion -> {
                    SQLViewRepresentation sql =
                        (SQLViewRepresentation)
                            viewVersion
                                .representations()
                                .get(viewVersion.representations().size() - 1);
                    return new BaseVersion(
                        viewVersion.versionId(),
                        viewVersion.versionId(),
                        viewVersion.timestampMillis(),
                        new VersionSummary(viewVersion.summary()),
                        ViewDefinition.of(
                            sql.sql(),
                            viewMetadata.schema(),
                            viewVersion.defaultCatalog(),
                            Arrays.asList(viewVersion.defaultNamespace().levels())));
                  })
              .collect(Collectors.toList());
      ViewVersionMetadata viewVersionMetadata =
          new ViewVersionMetadata(
              viewMetadata.location(),
              viewDefinition,
              viewMetadata.properties(),
              viewMetadata.currentVersionId(),
              versions,
              historyEntryList);
      return IcebergViewMetadataImplV0.of(viewVersionMetadata, viewMetadata.metadataFileLocation());
    }
    return icebergViewMetadata;
  }

  public static boolean isSupportedDialectForRead(String dialect) {
    if (Arrays.stream(IcebergViewMetadata.SupportedViewDialectsForRead.values())
        .anyMatch(e -> e.toString().equalsIgnoreCase(dialect))) {
      return true;
    }
    return false;
  }

  public static boolean isSupportedDialectForUpdate(String dialect) {
    if (Arrays.stream(IcebergViewMetadata.SupportedViewDialectsForWrite.values())
        .anyMatch(e -> e.toString().equalsIgnoreCase(dialect))) {
      return true;
    }
    return false;
  }
}
