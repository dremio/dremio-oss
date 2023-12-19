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

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.viewdepoc.BaseMetastoreViewOperations;
import org.apache.iceberg.viewdepoc.BaseView;
import org.apache.iceberg.viewdepoc.DDLOperations;
import org.apache.iceberg.viewdepoc.View;
import org.apache.iceberg.viewdepoc.ViewDefinition;
import org.apache.iceberg.viewdepoc.ViewOperations;
import org.apache.iceberg.viewdepoc.ViewUtils;
import org.apache.iceberg.viewdepoc.ViewVersionMetadata;
import org.projectnessie.error.NessieReferenceConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Retryer;
import com.dremio.common.util.Retryer.OperationFailedAfterRetriesException;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.plugins.NessieClient;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Generic class to operate on different views.
 */
public class IcebergNessieVersionedViews implements IcebergVersionedViews {
  private static final Logger logger = LoggerFactory.getLogger(IcebergNessieVersionedViews.class);

  private final String warehouseLocation;
  private final NessieClient nessieClient;
  private final Configuration fileSystemConfig;
  private final SupportsIcebergMutablePlugin plugin;
  private final String userName;
  private final Retryer retryer;

  private static final int RETRY_MAX = 5;
  private final Function<String, ViewVersionMetadata> metadataLoader;

  public static final String DIALECT = "DREMIO";

  public IcebergNessieVersionedViews(
      String warehouseLocation,
      NessieClient nessieClient,
      Configuration fileSystemConfig,
      SupportsIcebergMutablePlugin plugin,
      String userName,
      Function<String, ViewVersionMetadata> metadataLoader) {
    requireNonNull(warehouseLocation);
    this.warehouseLocation =
        warehouseLocation.endsWith("/")
            ? warehouseLocation.substring(0, warehouseLocation.length() - 1)
            : warehouseLocation;
    this.nessieClient = nessieClient;
    this.fileSystemConfig = fileSystemConfig;
    this.plugin = plugin;
    this.userName = userName;
    this.metadataLoader = metadataLoader;
    this.retryer = Retryer.newBuilder()
      .retryOnExceptionFunc(IcebergNessieVersionedViews::isRetriable)
      .setMaxRetries(RETRY_MAX)
      .build();
  }

  protected String defaultWarehouseLocation(List<String> viewKey) {
    return String.format("%s/%s", warehouseLocation, Joiner.on('/').join(viewKey));
  }

  @Override
  public void create(
      List<String> viewKey,
      ViewDefinition viewDefinition,
      Map<String, String> properties,
      ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.CREATE_VIEW);

    if (ops.current() != null) {
      throw UserException.concurrentModificationError()
          .message("View already exists: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    final int parentId = -1;
    final int versionId = 1;
    final String location = defaultWarehouseLocation(viewKey);

    ViewUtils.doCommit(
        DDLOperations.CREATE,
        properties,
        versionId,
        parentId,
        viewDefinition,
        location,
        ops,
        null); // Because we don't have prevViewVersionMetadata, pass null here.
  }

  @Override
  public void replace(
      List<String> viewKey,
      ViewDefinition viewDefinition,
      Map<String, String> properties,
      ResolvedVersionContext version) {
    try {
      retryer.run(() -> replaceView(viewKey, viewDefinition, properties, version));
    } catch (OperationFailedAfterRetriesException e) {
      //This exception is for retryer
      logger.info("Max retries reached, replace view query failed", e);
      throw new RuntimeException(e);
    }
  }

  private void replaceView(List<String> viewKey, ViewDefinition viewDefinition, Map<String, String> properties, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.ALTER_VIEW);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    ViewVersionMetadata prevViewVersionMetadata = ops.current();
    Preconditions.checkState(
        prevViewVersionMetadata.versions().size() > 0, "Version history not found");

    final int parentId = prevViewVersionMetadata.currentVersionId();
    final String location = defaultWarehouseLocation(viewKey);

    ViewUtils.doCommit(
        DDLOperations.REPLACE,
        properties,
        parentId + 1,
        parentId,
        viewDefinition,
        location,
        ops,
        prevViewVersionMetadata);
  }

  @Override
  public View load(List<String> viewKey, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.READ_ONLY);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    return new BaseView(ops, Joiner.on('.').join(viewKey));
  }

  @Override
  public ViewDefinition loadDefinition(List<String> viewKey, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.READ_ONLY);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    return ops.current().definition();
  }

  @Override
  public void drop(List<String> viewKey, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.DROP_VIEW);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    ops.drop(Joiner.on('.').join(viewKey));
  }

  protected BaseMetastoreViewOperations newViewOps(
    List<String> viewKey,
    ResolvedVersionContext version,
    @Nullable IcebergCommitOrigin commitOrigin
  ) {
    return new IcebergNessieVersionedViewOperations(
        plugin.createIcebergFileIO(
            plugin.getSystemUserFS(),
            null,
            null,
            null,
            null),
        nessieClient,
        viewKey,
        commitOrigin,
        DIALECT,
        version,
        userName,
        metadataLoader);
  }

  private static boolean isRetriable(Throwable e) {
    return e.getCause() instanceof NessieReferenceConflictException;
  }
}
