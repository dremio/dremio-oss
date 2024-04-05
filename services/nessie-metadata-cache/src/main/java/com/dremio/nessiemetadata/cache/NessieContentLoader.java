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
package com.dremio.nessiemetadata.cache;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieContentLoader {
  private static final Logger logger = LoggerFactory.getLogger(NessieContentLoader.class);

  private static Reference toRef(ResolvedVersionContext resolvedVersionContext) {
    Preconditions.checkNotNull(resolvedVersionContext);
    switch (resolvedVersionContext.getType()) {
      case BRANCH:
        return Branch.of(
            resolvedVersionContext.getRefName(), resolvedVersionContext.getCommitHash());
      case TAG:
        return Tag.of(resolvedVersionContext.getRefName(), resolvedVersionContext.getCommitHash());
      case COMMIT:
        return Detached.of(resolvedVersionContext.getCommitHash());
      default:
        throw new IllegalStateException("Unexpected value: " + resolvedVersionContext.getType());
    }
  }

  public static Optional<Content> loadNessieContent(
      NessieApiV2 nessieApi, ContentKey contentKey, ResolvedVersionContext version) {
    String logPrefix =
        String.format("Load of Nessie content (key: %s, version: %s)", contentKey, version);
    Content content;
    try {
      content =
          nessieApi.getContent().key(contentKey).reference(toRef(version)).get().get(contentKey);
    } catch (NessieNotFoundException e) {
      if (e.getErrorCode() == ErrorCode.CONTENT_NOT_FOUND) {
        logger.warn("{} returned CONTENT_NOT_FOUND", logPrefix);
        return Optional.empty();
      }
      logger.error("{} failed", logPrefix, e);
      throw UserException.dataReadError(e).buildSilently();
    }
    if (content == null) {
      logger.debug("{} returned null", logPrefix);
      return Optional.empty();
    }
    logger.debug(
        "{} returned content type: {}, content: {}", logPrefix, content.getType(), content);
    if (!(content instanceof IcebergTable
        || content instanceof IcebergView
        || content instanceof Namespace)) {
      logger.warn("{} returned unexpected content type: {} ", logPrefix, content.getType());
    }
    return Optional.of(content);
  }
}
