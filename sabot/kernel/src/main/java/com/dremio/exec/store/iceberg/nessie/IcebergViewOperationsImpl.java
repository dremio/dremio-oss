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
package com.dremio.exec.store.iceberg.nessie;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.IcebergViewMetadata.SupportedIcebergViewSpecVersion;
import com.dremio.exec.store.iceberg.IcebergViewMetadataImplV0;
import com.dremio.exec.store.iceberg.IcebergViewMetadataImplV1;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.exec.store.iceberg.viewdepoc.ViewDefinition;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadataParser;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieContent;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;

public class IcebergViewOperationsImpl implements IcebergViewOperations {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(IcebergViewOperationsImpl.class);
  IcebergViewMetadata.SupportedIcebergViewSpecVersion specVersion;
  String metadataWarehouseLocation;
  NessieClient nessieClient;
  String userName;
  String catalogName;
  FileIO fileIO;
  Function<String, IcebergViewMetadata> metadataLoader;
  IcebergNessieFilePathSanitizer pathSanitizer;

  IcebergViewOperationsImpl(
      IcebergViewMetadata.SupportedIcebergViewSpecVersion viewSpecVersion,
      String metadataWarehouseLocation,
      NessieClient nessieClient,
      String userName,
      String catalogName,
      FileIO fileIO,
      Function<String, IcebergViewMetadata> metadataLoader,
      IcebergNessieFilePathSanitizer pathSanitizer) {
    this.specVersion = viewSpecVersion;
    this.metadataWarehouseLocation = metadataWarehouseLocation;
    this.nessieClient = nessieClient;
    this.userName = userName;
    this.catalogName = catalogName;
    this.fileIO = fileIO;
    this.metadataLoader = metadataLoader;
    this.pathSanitizer = pathSanitizer;
  }

  @Override
  public void create(
      List<String> viewKey,
      String sql,
      Schema viewSchema,
      List<String> schemaPath,
      ResolvedVersionContext resolvedVersionContext) {
    Preconditions.checkNotNull(
        nessieClient, "NessieClient must be set for performing View create operation");
    Preconditions.checkNotNull(catalogName, "Catalog name must be for View create operation");
    Preconditions.checkState(
        specVersion != SupportedIcebergViewSpecVersion.UNKNOWN,
        "Unsupported View version : %s",
        specVersion.name());
    Preconditions.checkNotNull(
        metadataWarehouseLocation,
        "Location for iceberg metadata is not specified for View create operation");
    if (specVersion == IcebergViewMetadata.SupportedIcebergViewSpecVersion.V0) {
      final IcebergNessieVersionedViewsV0 versionedViews =
          new IcebergNessieVersionedViewsV0(
              metadataWarehouseLocation, nessieClient, fileIO, userName, metadataLoader);
      final ViewDefinition viewDefinition =
          ViewDefinition.of(sql, viewSchema, catalogName, schemaPath);

      versionedViews.create(
          viewKey, viewDefinition, Collections.emptyMap(), resolvedVersionContext, pathSanitizer);
    } else if (specVersion == SupportedIcebergViewSpecVersion.V1) {
      final IcebergNessieVersionedViewsV1 versionedViews =
          new IcebergNessieVersionedViewsV1(
              metadataWarehouseLocation, nessieClient, fileIO, userName, metadataLoader);
      List<ViewRepresentation> representations = new ArrayList<>();
      representations.add(
          ImmutableSQLViewRepresentation.builder()
              .dialect(IcebergViewMetadata.SupportedViewDialectsForRead.DREMIOSQL.toString())
              .sql(sql)
              .build());
      ViewVersion viewVersion =
          ImmutableViewVersion.builder()
              // VersionId for creating always defaults to 1 and monotonically increases.
              .versionId(1)
              .schemaId(viewSchema.schemaId())
              .addAllRepresentations(representations)
              .defaultNamespace(Namespace.of(schemaPath.toArray(new String[schemaPath.size()])))
              .defaultCatalog(catalogName)
              .timestampMillis(System.currentTimeMillis())
              .putAllSummary(EnvironmentContext.get())
              .build();

      versionedViews.create(
          viewKey,
          viewVersion,
          Collections.emptyMap(),
          resolvedVersionContext,
          viewSchema,
          pathSanitizer);
    }
  }

  @Override
  public void update(
      List<String> viewKey,
      String viewSql,
      Schema viewSchema,
      List<String> schemaPath,
      Map<String, String> viewProperties,
      ResolvedVersionContext version) {
    Preconditions.checkNotNull(
        nessieClient, "NessieClient must be set for performing View update operation");
    Preconditions.checkNotNull(catalogName, "Catalog name must be set for View update operation");
    Preconditions.checkNotNull(
        metadataWarehouseLocation,
        "Location for iceberg metadata is not specified for View update operation");
    Preconditions.checkState(
        specVersion != SupportedIcebergViewSpecVersion.UNKNOWN,
        "Unsupported View version : %s",
        specVersion.name());
    if (specVersion == IcebergViewMetadata.SupportedIcebergViewSpecVersion.V0) {
      final IcebergNessieVersionedViewsV0 versionedViews =
          new IcebergNessieVersionedViewsV0(
              metadataWarehouseLocation, nessieClient, fileIO, userName, metadataLoader);
      final ViewDefinition viewDefinition =
          ViewDefinition.of(viewSql, viewSchema, catalogName, schemaPath);
      versionedViews.replace(viewKey, viewDefinition, viewProperties, version, pathSanitizer);
    } else if (specVersion == SupportedIcebergViewSpecVersion.V1) {
      final IcebergNessieVersionedViewsV1 versionedViews =
          new IcebergNessieVersionedViewsV1(
              metadataWarehouseLocation, nessieClient, fileIO, userName, metadataLoader);

      ViewOperations ops =
          versionedViews.newViewOps(viewKey, version, IcebergCommitOrigin.ALTER_VIEW, fileIO);
      if (null == ops.current()) {
        throw new NoSuchViewException("View does not exist: %s", Joiner.on('.').join(viewKey));
      }

      ViewMetadata viewMetadata = ops.current();
      int maxVersionId =
          viewMetadata.versions().stream()
              .map(ViewVersion::versionId)
              .max(Integer::compareTo)
              .orElseGet(viewMetadata::currentVersionId);

      List<ViewRepresentation> representations = new ArrayList<>();
      representations.add(
          ImmutableSQLViewRepresentation.builder()
              .dialect(IcebergViewMetadata.SupportedViewDialectsForRead.DREMIOSQL.toString())
              .sql(viewSql)
              .build());

      ViewVersion viewVersion =
          ImmutableViewVersion.builder()
              .versionId(maxVersionId + 1)
              .schemaId(viewSchema.schemaId())
              .addAllRepresentations(representations)
              .defaultNamespace(Namespace.of(schemaPath.toArray(new String[schemaPath.size()])))
              .defaultCatalog(catalogName)
              .timestampMillis(System.currentTimeMillis())
              .putAllSummary(EnvironmentContext.get())
              .build();
      versionedViews.update(viewKey, viewVersion, viewProperties, viewSchema, ops, pathSanitizer);
    }
  }

  @Override
  public void drop(List<String> viewKey, ResolvedVersionContext version) {
    Preconditions.checkNotNull(
        nessieClient, "NessieClient must be set for performing View drop operation");
    Preconditions.checkNotNull(
        metadataWarehouseLocation,
        "Location for iceberg metadata is not specified for View drop operation");
    // So the version of the underlying iceberg view in storage should not matter.

    final IcebergNessieVersionedViewsV0 versionedViews =
        new IcebergNessieVersionedViewsV0(
            metadataWarehouseLocation, nessieClient, fileIO, userName, metadataLoader);
    versionedViews.drop(viewKey, version);
  }

  @Override
  public IcebergViewMetadata refreshFromMetadataLocation(String metadataLocation) {
    IcebergViewMetadata.SupportedIcebergViewSpecVersion specVersion =
        IcebergUtils.findIcebergViewVersion(metadataLocation, fileIO);
    InputFile inputFile = fileIO.newInputFile(metadataLocation);
    switch (specVersion) {
      case V0:
        return IcebergViewMetadataImplV0.of(
            ViewVersionMetadataParser.read(inputFile), metadataLocation);
      case V1:
        return IcebergViewMetadataImplV1.of(ViewMetadataParser.read(inputFile));
      case UNKNOWN:
      default:
        throw new RuntimeIOException(
            "Unsupported view metadata json was found at location %s", metadataLocation);
    }
  }

  @Override
  public IcebergViewMetadata refresh(
      List<String> viewKey, ResolvedVersionContext resolvedVersionContext) {
    Preconditions.checkNotNull(
        nessieClient, "NessieClient must be set for performing View refresh operation");
    final String metadataLocation =
        getMetadataLocation(viewKey, resolvedVersionContext)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Failed to determine metadataLocation: "
                            + viewKey
                            + " version: "
                            + resolvedVersionContext));
    return refreshFromMetadataLocation(metadataLocation);
  }

  private Optional<String> getMetadataLocation(
      List<String> catalogKey, ResolvedVersionContext resolvedVersionContext) {
    return nessieClient
        .getContent(catalogKey, resolvedVersionContext, null)
        .flatMap(NessieContent::getMetadataLocation);
  }
}
