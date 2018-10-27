/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store;

import java.util.Optional;

import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.google.common.base.Preconditions;

/**
 * Dataset retrieval options.
 */
public class DatasetRetrievalOptions {

  private static final Optional<Boolean> DEFAULT_AUTO_PROMOTE_OPTIONAL =
      Optional.ofNullable(System.getProperty("dremio.datasets.auto_promote"))
          .map(Boolean::parseBoolean);

  static final boolean DEFAULT_AUTO_PROMOTE;
  public static final DatasetRetrievalOptions DEFAULT;
  public static final DatasetRetrievalOptions IGNORE_AUTHZ_ERRORS;

  static {
    DEFAULT_AUTO_PROMOTE = DEFAULT_AUTO_PROMOTE_OPTIONAL.orElse(false);

    DEFAULT = newBuilder()
        .setIgnoreAuthzErrors(false)
        .setDeleteUnavailableDatasets(true)
        .setAutoPromote(DEFAULT_AUTO_PROMOTE)
        .setForceUpdate(false)
        .build();

    IGNORE_AUTHZ_ERRORS =
        DEFAULT.toBuilder()
            .setIgnoreAuthzErrors(true)
            .build();
  }

  private final Optional<Boolean> ignoreAuthzErrors;
  private final Optional<Boolean> deleteUnavailableDatasets;
  private final Optional<Boolean> autoPromote;
  private final Optional<Boolean> forceUpdate;

  private DatasetRetrievalOptions fallback;

  /**
   * Use {@link #newBuilder() builder}.
   */
  private DatasetRetrievalOptions(
      Boolean ignoreAuthzErrors,
      Boolean deleteUnavailableDatasets,
      Boolean autoPromote,
      Boolean forceUpdate
  ) {
    this.ignoreAuthzErrors = Optional.ofNullable(ignoreAuthzErrors);
    this.deleteUnavailableDatasets = Optional.ofNullable(deleteUnavailableDatasets);
    this.autoPromote = Optional.ofNullable(autoPromote);
    this.forceUpdate = Optional.ofNullable(forceUpdate);
  }

  public boolean ignoreAuthzErrors() {
    return ignoreAuthzErrors.orElseGet(() -> fallback.ignoreAuthzErrors());

  }

  public boolean deleteUnavailableDatasets() {
    return deleteUnavailableDatasets.orElseGet(() -> fallback.deleteUnavailableDatasets());
  }

  public boolean autoPromote() {
    return autoPromote.orElseGet(() -> fallback.autoPromote());
  }

  public boolean forceUpdate() {
    return forceUpdate.orElseGet(() -> fallback.forceUpdate());
  }

  public DatasetRetrievalOptions withFallback(DatasetRetrievalOptions fallback) {
    this.fallback = fallback;
    return this;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setIgnoreAuthzErrors(ignoreAuthzErrors.orElse(null))
        .setDeleteUnavailableDatasets(deleteUnavailableDatasets.orElse(null))
        .setAutoPromote(autoPromote.orElse(null))
        .setForceUpdate(forceUpdate.orElse(null));
  }

  public static class Builder {

    private Boolean ignoreAuthzErrors;
    private Boolean deleteUnavailableDatasets;
    private Boolean autoPromote;
    private Boolean forceUpdate;

    private Builder() {
    }

    public Builder setIgnoreAuthzErrors(Boolean ignoreAuthzErrors) {
      this.ignoreAuthzErrors = ignoreAuthzErrors;
      return this;
    }

    public Builder setDeleteUnavailableDatasets(Boolean deleteUnavailableDatasets) {
      this.deleteUnavailableDatasets = deleteUnavailableDatasets;
      return this;
    }

    public Builder setAutoPromote(Boolean autoPromote) {
      this.autoPromote = autoPromote;
      return this;
    }

    public Builder setForceUpdate(Boolean forceUpdate) {
      this.forceUpdate = forceUpdate;
      return this;
    }

    public DatasetRetrievalOptions build() {
      return new DatasetRetrievalOptions(ignoreAuthzErrors, deleteUnavailableDatasets, autoPromote, forceUpdate);
    }
  }

  /**
   * Creates a {@link Builder}.
   *
   * @return new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Convert a {@link MetadataPolicy} to {@link DatasetRetrievalOptions}.
   *
   * @param policy metadata policy
   * @return dataset retrieval options
   */
  public static DatasetRetrievalOptions fromMetadataPolicy(MetadataPolicy policy) {
    Preconditions.checkNotNull(policy.getAutoPromoteDatasets());
    Preconditions.checkNotNull(policy.getDeleteUnavailableDatasets());

    return newBuilder()
        .setAutoPromote(policy.getAutoPromoteDatasets())
        .setDeleteUnavailableDatasets(policy.getDeleteUnavailableDatasets())
        // not an option in policy (or UI)
        .setForceUpdate(false)
        // not an option in policy (or UI)
        .setIgnoreAuthzErrors(false)
        .build();
  }
}
