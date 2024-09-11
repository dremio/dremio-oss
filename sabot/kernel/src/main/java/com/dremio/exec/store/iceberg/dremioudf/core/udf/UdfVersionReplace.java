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
package com.dremio.exec.store.iceberg.dremioudf.core.udf;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import com.dremio.exec.store.iceberg.dremioudf.api.udf.ReplaceUdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class UdfVersionReplace implements ReplaceUdfVersion {
  private final UdfOperations ops;
  private final List<UdfRepresentation> representations = Lists.newArrayList();
  private UdfMetadata base;
  private Namespace defaultNamespace = null;
  private String defaultCatalog = null;
  private UdfSignature signature = null;

  UdfVersionReplace(UdfOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public UdfVersion apply() {
    return internalApply().currentVersion();
  }

  UdfMetadata internalApply() {
    Preconditions.checkState(
        !representations.isEmpty(), "Cannot replace UDF without specifying a body");
    Preconditions.checkState(null != signature, "Cannot replace UDF without specifying signature");
    Preconditions.checkState(
        null != defaultNamespace, "Cannot replace UDF without specifying a default namespace");

    this.base = ops.refresh();

    UdfVersion newVersion =
        ImmutableUdfVersion.builder()
            .versionId(UdfUtil.generateUUID())
            .timestampMillis(System.currentTimeMillis())
            .signatureId(signature.signatureId())
            .defaultNamespace(defaultNamespace)
            .defaultCatalog(defaultCatalog)
            .putAllSummary(EnvironmentContext.get())
            .addAllRepresentations(representations)
            .build();

    return UdfMetadata.buildFrom(base).setCurrentVersion(newVersion, signature).build();
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(
            PropertyUtil.propertyAsInt(
                base.properties(), COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            PropertyUtil.propertyAsInt(
                base.properties(), COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(
                base.properties(), COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(
                base.properties(), COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(taskOps -> taskOps.commit(base, internalApply()));
  }

  @Override
  public ReplaceUdfVersion withSignature(UdfSignature signature) {
    this.signature = signature;
    return this;
  }

  @Override
  public ReplaceUdfVersion withBody(String dialect, String sql, String comment) {
    representations.add(
        ImmutableSQLUdfRepresentation.builder()
            .dialect(dialect)
            .body(sql)
            .comment(comment)
            .build());
    return this;
  }

  @Override
  public ReplaceUdfVersion withDefaultCatalog(String catalog) {
    this.defaultCatalog = catalog;
    return this;
  }

  @Override
  public ReplaceUdfVersion withDefaultNamespace(Namespace namespace) {
    this.defaultNamespace = namespace;
    return this;
  }
}
