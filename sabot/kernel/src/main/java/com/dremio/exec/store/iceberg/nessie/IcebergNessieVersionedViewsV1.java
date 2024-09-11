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

import static com.dremio.exec.store.iceberg.IcebergViewMetadataUtils.isSupportedDialectForUpdate;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VersionedPlugin.EntityType;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.plugins.NessieClient;
import com.google.common.base.Joiner;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewProperties;
import org.apache.iceberg.view.ViewVersion;

public class IcebergNessieVersionedViewsV1 {
  private final String warehouseLocation;
  private final NessieClient nessieClient;
  private final String userName;
  private final Function<String, IcebergViewMetadata> metadataLoader;
  private FileIO fileIO;

  public IcebergNessieVersionedViewsV1(
      String warehouseLocation,
      NessieClient nessieClient,
      FileIO fileIO,
      String userName,
      Function<String, IcebergViewMetadata> metadataLoader) {
    this.warehouseLocation =
        warehouseLocation.endsWith("/")
            ? warehouseLocation.substring(0, warehouseLocation.length() - 1)
            : warehouseLocation;
    this.nessieClient = nessieClient;
    this.userName = userName;
    this.metadataLoader = metadataLoader;
    this.fileIO = fileIO;
  }

  public void create(
      List<String> viewKey,
      ViewVersion viewVersion,
      Map<String, String> properties,
      ResolvedVersionContext resolvedVersionContext,
      Schema viewSchema,
      IcebergNessieFilePathSanitizer pathSanitizer) {
    ViewOperations ops =
        newViewOps(viewKey, resolvedVersionContext, IcebergCommitOrigin.CREATE_VIEW, fileIO);

    if (ops.current() != null) {
      throw UserException.concurrentModificationError()
          .message("View already exists: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setProperties(properties)
            .setLocation(defaultWarehouseLocation(pathSanitizer.getPath(viewKey)))
            .setCurrentVersion(viewVersion, viewSchema)
            .build();

    try {
      ops.commit(null, viewMetadata);
    } catch (CommitFailedException ignored) {
      throw new AlreadyExistsException(
          "View was created concurrently: %s", Joiner.on('.').join(viewKey));
    }
  }

  public void update(
      List<String> viewKey,
      ViewVersion viewVersion,
      Map<String, String> properties,
      Schema schema,
      ViewOperations viewOperations,
      IcebergNessieFilePathSanitizer pathSanitizer) {

    ViewMetadata viewMetadata = viewOperations.current();

    // Check if dialect of the underlying view is different. If so, also check if it's updatable by
    // Dremio engine.
    String baseViewDialect = getDialect(viewMetadata);
    if (!baseViewDialect.equalsIgnoreCase(
            IcebergViewMetadata.SupportedViewDialectsForRead.DREMIOSQL.toString())
        && (isSupportedDialectForUpdate(baseViewDialect))) {
      properties.put(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "true");
    }
    ViewMetadata.Builder builder =
        ViewMetadata.buildFrom(viewMetadata)
            .setProperties(properties)
            .setCurrentVersion(viewVersion, schema);

    String location = defaultWarehouseLocation(pathSanitizer.getPath(viewKey));
    if (null != location) {
      builder.setLocation(location);
    }

    ViewMetadata replacement = builder.build();
    try {
      viewOperations.commit(viewMetadata, replacement);
    } catch (CommitFailedException ignored) {
      throw new AlreadyExistsException(
          "View was updated concurrently: %s", Joiner.on('.').join(viewKey));
    }
  }

  public void drop(List<String> viewKey, ResolvedVersionContext version) {
    nessieClient.deleteCatalogEntry(viewKey, EntityType.ICEBERG_VIEW, version, userName);
  }

  protected BaseViewOperations newViewOps(
      List<String> viewKey,
      ResolvedVersionContext version,
      @Nullable IcebergCommitOrigin commitOrigin,
      FileIO fileIO) {
    return new IcebergNessieVersionedViewOperationsV1(
        fileIO, nessieClient, viewKey, commitOrigin, version, userName, metadataLoader);
  }

  protected String defaultWarehouseLocation(List<String> viewKey) {
    return String.format("%s/%s", warehouseLocation, Joiner.on('/').join(viewKey));
  }

  private String getDialect(ViewMetadata viewMetadata) {
    SQLViewRepresentation sqlViewRepresentation =
        (SQLViewRepresentation)
            viewMetadata
                .currentVersion()
                .representations()
                .get(viewMetadata.currentVersion().representations().size() - 1);
    return sqlViewRepresentation.dialect();
  }
}
