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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Retryer;
import com.dremio.common.util.Retryer.OperationFailedAfterRetriesException;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.exec.store.iceberg.viewdepoc.BaseMetastoreViewOperations;
import com.dremio.exec.store.iceberg.viewdepoc.BaseView;
import com.dremio.exec.store.iceberg.viewdepoc.DDLOperations;
import com.dremio.exec.store.iceberg.viewdepoc.View;
import com.dremio.exec.store.iceberg.viewdepoc.ViewDefinition;
import com.dremio.exec.store.iceberg.viewdepoc.ViewOperations;
import com.dremio.exec.store.iceberg.viewdepoc.ViewUtils;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadata;
import com.dremio.plugins.NessieClient;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.error.NessieReferenceConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generic class to operate on V0 version Iceberg (private implementation) views. */
public class IcebergNessieVersionedViewsV0 {
  private static final Logger logger = LoggerFactory.getLogger(IcebergNessieVersionedViewsV0.class);

  private final String warehouseLocation;
  private final NessieClient nessieClient;
  private final String userName;
  private final Retryer retryer;

  private static final int RETRY_MAX = 5;
  private final Function<String, IcebergViewMetadata> metadataLoader;
  private FileIO fileIO;

  public IcebergNessieVersionedViewsV0(
      String warehouseLocation,
      NessieClient nessieClient,
      FileIO fileIO,
      String userName,
      Function<String, IcebergViewMetadata> metadataLoader) {
    requireNonNull(warehouseLocation);
    this.warehouseLocation =
        warehouseLocation.endsWith("/")
            ? warehouseLocation.substring(0, warehouseLocation.length() - 1)
            : warehouseLocation;
    this.nessieClient = nessieClient;
    this.userName = userName;
    this.retryer =
        Retryer.newBuilder()
            .retryOnExceptionFunc(IcebergNessieVersionedViewsV0::isRetriable)
            .setMaxRetries(RETRY_MAX)
            .build();
    this.metadataLoader = metadataLoader;
    this.fileIO = fileIO;
  }

  protected String defaultWarehouseLocation(List<String> viewKey) {
    return String.format("%s/%s", warehouseLocation, Joiner.on('/').join(viewKey));
  }

  public void create(
      List<String> viewKey,
      ViewDefinition viewDefinition,
      Map<String, String> properties,
      ResolvedVersionContext version,
      IcebergNessieFilePathSanitizer pathSanitizer) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.CREATE_VIEW, fileIO);

    if (ops.current() != null) {
      throw UserException.concurrentModificationError()
          .message("View already exists: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    final int parentId = -1;
    final int versionId = 1;
    final String location = defaultWarehouseLocation(pathSanitizer.getPath(viewKey));

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

  public void replace(
      List<String> viewKey,
      ViewDefinition viewDefinition,
      Map<String, String> properties,
      ResolvedVersionContext version,
      IcebergNessieFilePathSanitizer pathSanitizer) {
    try {
      retryer.run(() -> replaceView(viewKey, viewDefinition, properties, version, pathSanitizer));
    } catch (OperationFailedAfterRetriesException e) {
      // This exception is for retryer
      logger.info("Max retries reached, replace view query failed", e);
      throw new RuntimeException(e);
    }
  }

  private void replaceView(
      List<String> viewKey,
      ViewDefinition viewDefinition,
      Map<String, String> properties,
      ResolvedVersionContext version,
      IcebergNessieFilePathSanitizer pathSanitizer) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.ALTER_VIEW, fileIO);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    ViewVersionMetadata prevViewVersionMetadata = ops.current();
    Preconditions.checkState(
        prevViewVersionMetadata.versions().size() > 0, "Version history not found");

    final int parentId = prevViewVersionMetadata.currentVersionId();
    final String location = defaultWarehouseLocation(pathSanitizer.getPath(viewKey));

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

  public View load(List<String> viewKey, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.READ_ONLY, fileIO);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    return new BaseView(ops, Joiner.on('.').join(viewKey));
  }

  public ViewDefinition loadDefinition(List<String> viewKey, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.READ_ONLY, fileIO);

    if (ops.current() == null) {
      throw UserException.dataReadError()
          .message("View not found: %s", Joiner.on('.').join(viewKey))
          .buildSilently();
    }

    return ops.current().definition();
  }

  public void drop(List<String> viewKey, ResolvedVersionContext version) {
    ViewOperations ops = newViewOps(viewKey, version, IcebergCommitOrigin.DROP_VIEW, fileIO);
    ops.drop(Joiner.on('.').join(viewKey));
  }

  protected BaseMetastoreViewOperations newViewOps(
      List<String> viewKey,
      ResolvedVersionContext version,
      @Nullable IcebergCommitOrigin commitOrigin,
      FileIO fileIO) {
    return new IcebergNessieVersionedViewOperationsV0(
        fileIO,
        nessieClient,
        viewKey,
        commitOrigin,
        IcebergViewMetadata.SupportedViewDialectsForRead.DREMIO,
        version,
        userName,
        metadataLoader);
  }

  private static boolean isRetriable(Throwable e) {
    return e.getCause() instanceof NessieReferenceConflictException;
  }
}
