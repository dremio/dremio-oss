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
import java.util.function.Predicate;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseMetastoreViewOperations;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.apache.iceberg.view.ViewVersionMetadataParser;
import org.projectnessie.model.IcebergView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.plugins.NessieClient;
import com.google.common.base.Preconditions;

/**
 * Versioned iceberg view operations.
 */
public class IcebergNessieVersionedViewOperations extends BaseMetastoreViewOperations {
  private static final Logger logger =
      LoggerFactory.getLogger(IcebergNessieVersionedViewOperations.class);
  private static final Predicate<Exception> RETRY_IF =
      exec -> !exec.getClass().getCanonicalName().contains("Unrecoverable");
  private static final int MAX_RETRIES = 2;

  private final FileIO fileIO;
  private final NessieClient nessieClient;
  private final List<String> viewKey;
  private final String dialect;
  private final ResolvedVersionContext version;
  private final String userName;
  private IcebergView icebergView;
  private String baseContentId;

  public IcebergNessieVersionedViewOperations(
      FileIO fileIO,
      NessieClient nessieClient,
      List<String> viewKey,
      String dialect,
      ResolvedVersionContext version,
      String userName) {
    this.fileIO = fileIO;
    this.nessieClient = requireNonNull(nessieClient);
    this.viewKey = requireNonNull(viewKey);
    this.dialect = dialect;
    this.version = version;
    this.baseContentId = null;
    this.userName = userName;
  }

  @Override
  public ViewVersionMetadata refresh() {
    baseContentId = nessieClient.getContentId(viewKey, version, null);
    String metadataLocation = null;
    if (baseContentId != null) {
      metadataLocation = nessieClient.getMetadataLocation(viewKey, version, null);
      Preconditions.checkState(metadataLocation != null,
        "No metadataLocation for iceberg view: " + viewKey + " ref: " + version);
    }
    refreshFromMetadataLocation(metadataLocation, RETRY_IF, MAX_RETRIES, this::loadViewMetadata);

    return current();
  }

  private ViewVersionMetadata loadViewMetadata(String metadataLocation) {
    logger.debug("Loading view metadata from location {} ", metadataLocation);
    return ViewVersionMetadataParser.read(io().newInputFile(metadataLocation));
  }

  @Override
  public void drop(String viewIdentifier) {
    logger.debug("Deleting key for view {} at version {} from Nessie ", viewKey, version);
    nessieClient.deleteCatalogEntry(viewKey, version, userName);
  }

  @Override
  public void commit(
      ViewVersionMetadata base,
      ViewVersionMetadata target,
      Map<String, String> properties) {
    final String newMetadataLocation = writeNewMetadata(target, currentVersion() + 1);

    boolean isFailedOperation = true;
    try {
      nessieClient.commitView(viewKey, newMetadataLocation, icebergView, target, dialect, version, baseContentId, userName);
      isFailedOperation = false;
    } finally {
      if (isFailedOperation) {
        logger.debug("Deleting metadata file {} of view {}", newMetadataLocation, viewKey);
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
