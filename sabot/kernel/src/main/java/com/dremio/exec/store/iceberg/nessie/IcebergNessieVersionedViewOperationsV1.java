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

import static com.dremio.exec.store.iceberg.IcebergViewMetadataUtils.translateVersion;
import static java.util.Objects.requireNonNull;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.IcebergViewMetadata.SupportedIcebergViewSpecVersion;
import com.dremio.exec.store.iceberg.IcebergViewMetadataImplV1;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieContent;
import com.dremio.plugins.NessieViewAdapter;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergNessieVersionedViewOperationsV1 extends BaseViewOperations {
  private static final Logger logger =
      LoggerFactory.getLogger(IcebergNessieVersionedViewOperationsV1.class);
  private static final int MAX_RETRIES = 2;
  private static final Predicate<Exception> RETRY_IF =
      exec -> !exec.getClass().getCanonicalName().contains("Unrecoverable");
  private final FileIO fileIO;
  private final NessieClient nessieClient;
  private final List<String> viewKey;
  private final IcebergCommitOrigin commitOrigin;
  private ResolvedVersionContext resolvedVersionContext;
  private final String userName;
  private final Function<String, IcebergViewMetadata> metadataLoader;
  private String baseContentId;
  private int retryCount;

  public IcebergNessieVersionedViewOperationsV1(
      FileIO fileIO,
      NessieClient nessieClient,
      List<String> viewKey,
      IcebergCommitOrigin commitOrigin,
      ResolvedVersionContext resolvedVersionContext,
      String userName,
      Function<String, IcebergViewMetadata> metadataLoader) {
    this.fileIO = fileIO;
    this.nessieClient = requireNonNull(nessieClient);
    this.viewKey = requireNonNull(viewKey);
    this.commitOrigin = commitOrigin;
    this.resolvedVersionContext = resolvedVersionContext;
    this.baseContentId = null;
    this.userName = userName;
    this.metadataLoader = metadataLoader;
    this.retryCount = 0;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    if (resolvedVersionContext.isBranch()) {
      resolvedVersionContext =
          nessieClient.resolveVersionContext(
              VersionContext.ofBranch(resolvedVersionContext.getRefName()));
    }
    try {
      Optional<NessieContent> maybeNessieContent =
          nessieClient.getContent(viewKey, resolvedVersionContext, null);
      if (maybeNessieContent.isEmpty()) {
        if (currentMetadataLocation() != null) {
          throw new NoSuchViewException(
              "View does not exist: %s in %s", viewKey, resolvedVersionContext);
        }
      } else {
        NessieContent nessieContent = maybeNessieContent.get();
        baseContentId = nessieContent.getContentId();
        metadataLocation =
            nessieContent
                .getMetadataLocation()
                .orElseThrow(
                    () ->
                        new NessieContentNotFoundException(
                            ContentKey.of(viewKey), resolvedVersionContext.getRefName()));
      }
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException(
            "View does not exist: %s in %s", viewKey, resolvedVersionContext);
      }
    }
    refreshFromMetadataLocation(metadataLocation, RETRY_IF, MAX_RETRIES, this::viewLoader);
  }

  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata target) {
    String newMetadataLocation = writeNewMetadataIfRequired(target);
    boolean failure = true;
    try {
      SQLViewRepresentation sqlViewRepresentation =
          (SQLViewRepresentation)
              target
                  .currentVersion()
                  .representations()
                  .get(target.currentVersion().representations().size() - 1);
      NessieViewAdapter nessieViewAdapter =
          new NessieViewAdapter(
              target.currentVersionId(), target.currentSchemaId(), sqlViewRepresentation.sql());
      nessieClient.commitView(
          viewKey,
          newMetadataLocation,
          nessieViewAdapter,
          resolvedVersionContext,
          baseContentId,
          commitOrigin,
          userName);
      failure = false;
    } catch (ReferenceConflictException e) {
      if (e.getCause() instanceof NessieConflictException && retryCount < 4) {
        logger.debug(
            String.format("Retrying commit for view %s because of conflict exception", viewKey));
        retryCount += 1;
        doRefresh();
        doCommit(base, target);
      } else {
        throw new IllegalStateException(e);
      }
    } finally {
      if (failure) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  @Override
  protected String viewName() {
    return String.join(".", viewKey);
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }

  private ViewMetadata viewLoader(String metadataLocation) {
    IcebergViewMetadata icebergViewMetadata = metadataLoader.apply(metadataLocation);
    icebergViewMetadata = translateVersion(icebergViewMetadata, SupportedIcebergViewSpecVersion.V1);
    return IcebergViewMetadataImplV1.getViewMetadata(icebergViewMetadata);
  }
}
