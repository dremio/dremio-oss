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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.view.BaseMetastoreViewOperations;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.DDLOperations;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewDefinition;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewUtils;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
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

  public static final String DIALECT = "DREMIO";

  public IcebergNessieVersionedViews(
      String warehouseLocation,
      NessieClient nessieClient,
      Configuration fileSystemConfig,
      SupportsIcebergMutablePlugin plugin,
      String userName) {
    requireNonNull(warehouseLocation);
    this.warehouseLocation =
        warehouseLocation.endsWith("/")
            ? warehouseLocation.substring(0, warehouseLocation.length() - 1)
            : warehouseLocation;
    this.nessieClient = nessieClient;
    this.fileSystemConfig = fileSystemConfig;
    this.plugin = plugin;
    this.userName = userName;
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
    ViewOperations ops = newViewOps(viewKey, version);

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
    ViewOperations ops = newViewOps(viewKey, version);

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
    ViewOperations ops = newViewOps(viewKey, version);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    return new BaseView(ops, Joiner.on('.').join(viewKey));
  }

  @Override
  public ViewDefinition loadDefinition(List<String> viewKey, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    return ops.current().definition();
  }

  @Override
  public void drop(List<String> viewKey, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    ops.drop(Joiner.on('.').join(viewKey));
  }

  protected BaseMetastoreViewOperations newViewOps(
      List<String> viewKey, ResolvedVersionContext version) {
    return new IcebergNessieVersionedViewOperations(
        plugin.createIcebergFileIO(plugin.getSystemUserFS(), null, null, null, null),
        nessieClient, viewKey, DIALECT, version, userName);
  }
}
