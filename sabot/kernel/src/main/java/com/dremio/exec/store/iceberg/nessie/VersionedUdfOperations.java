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
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.store.iceberg.dremioudf.api.catalog.NoSuchUdfException;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.BaseUdfOperations;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfMetadata;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieContent;
import com.dremio.plugins.NessieUdfAdapter;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;

public class VersionedUdfOperations extends BaseUdfOperations {
  private static final int MAX_RETRIES = 2;
  private static final Predicate<Exception> RETRY_IF =
      exec -> !exec.getClass().getCanonicalName().contains("Unrecoverable");
  private final FileIO fileIO;
  private final NessieClient nessieClient;
  private final List<String> udfKey;
  private final String userName;
  private ResolvedVersionContext versionContext;
  private String baseContentId;
  private IcebergCommitOrigin commitOrigin;

  public VersionedUdfOperations(
      FileIO fileIO,
      NessieClient nessieClient,
      List<String> udfKey,
      ResolvedVersionContext versionContext,
      String userName) {
    this.fileIO = fileIO;
    this.nessieClient = requireNonNull(nessieClient);
    this.udfKey = requireNonNull(udfKey);
    this.versionContext = versionContext;
    this.baseContentId = null;
    this.commitOrigin = null;
    this.userName = userName;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    if (versionContext.isBranch()) {
      versionContext =
          nessieClient.resolveVersionContext(VersionContext.ofBranch(versionContext.getRefName()));
    }
    try {
      Optional<NessieContent> maybeNessieContent =
          nessieClient.getContent(udfKey, versionContext, null);
      if (maybeNessieContent.isEmpty()) {
        if (currentMetadataLocation() != null) {
          throw new NoSuchUdfException("UDF does not exist: %s in %s", udfKey, versionContext);
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
                            ContentKey.of(udfKey), versionContext.getRefName()));
      }
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchUdfException("UDF does not exist: %s in %s", udfKey, versionContext);
      }
    }
    refreshFromMetadataLocation(metadataLocation, RETRY_IF, MAX_RETRIES);
  }

  @Override
  protected void doCommit(UdfMetadata base, UdfMetadata metadata) {

    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    boolean failure = true;
    try {
      SQLUdfRepresentation sqlUdfRepresentation =
          (SQLUdfRepresentation)
              metadata
                  .currentVersion()
                  .representations()
                  .get(metadata.currentVersion().representations().size() - 1);
      NessieUdfAdapter nessieUdfAdapter =
          new NessieUdfAdapter(metadata.currentVersionId(), metadata.currentSignatureId());
      nessieClient.commitUdf(
          udfKey,
          newMetadataLocation,
          nessieUdfAdapter,
          versionContext,
          baseContentId,
          commitOrigin,
          userName);
      failure = false;
    } finally {
      if (failure) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  @Override
  protected String udfName() {
    return String.join(".", udfKey);
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }

  protected VersionedUdfOperations withCommitOrigin(IcebergCommitOrigin commitOrigin) {
    this.commitOrigin = commitOrigin;
    return this;
  }
}
