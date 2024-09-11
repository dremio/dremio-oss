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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.store.iceberg.VersionedUdfMetadata;
import com.dremio.exec.store.iceberg.dremioudf.api.catalog.NoSuchUdfException;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.Udf;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfBuilder;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.BaseUdf;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.ImmutableSQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.ImmutableUdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfMetadata;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfOperations;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfUtil;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.plugins.NessieClient;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.model.ContentKey;

public class VersionedUdfBuilder implements UdfBuilder {

  private final List<String> udfKey;
  private ResolvedVersionContext versionContext;
  private final Map<String, String> properties = Maps.newHashMap();
  private final List<UdfRepresentation> representations = Lists.newArrayList();
  private Namespace defaultNamespace = null;
  private String defaultCatalog = null;
  private final List<UdfSignature> signatures = Lists.newArrayList();
  private String warehouseLocation = null;
  private UdfSignature signature = null;
  private String location = null;
  private NessieClient nessieClient;
  private String userName;
  private FileIO fileIO;

  public VersionedUdfBuilder(List<String> udfKey) {
    this.udfKey = udfKey;
  }

  public VersionedUdfBuilder() {
    this(null);
  }

  @Override
  public VersionedUdfBuilder withDefaultCatalog(String catalog) {
    this.defaultCatalog = catalog;
    return this;
  }

  @Override
  public VersionedUdfBuilder withDefaultNamespace(Namespace namespace) {
    this.defaultNamespace = namespace;
    return this;
  }

  @Override
  public VersionedUdfBuilder withProperties(Map<String, String> newProperties) {
    this.properties.putAll(newProperties);
    return this;
  }

  @Override
  public VersionedUdfBuilder withProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  @Override
  public VersionedUdfBuilder withLocation(String newLocation) {
    this.location = newLocation;
    return this;
  }

  @Override
  public VersionedUdfBuilder withSignature(UdfSignature newSignature) {
    this.signature = newSignature;
    return this;
  }

  @Override
  public VersionedUdfBuilder withBody(String dialect, String sql, String comment) {
    Preconditions.checkState(
        dialect == VersionedUdfMetadata.SupportedUdfDialects.DREMIOSQL.toString(),
        "Unsupported Udf dialect : %s. Supported dialects are : %s",
        dialect,
        VersionedUdfMetadata.SupportedUdfDialects.DREMIOSQL.toString());
    representations.add(
        ImmutableSQLUdfRepresentation.builder()
            .dialect(dialect)
            .body(sql)
            .comment(comment)
            .build());
    return this;
  }

  public VersionedUdfBuilder withWarehouseLocation(String warehouseLocation) {
    this.warehouseLocation =
        warehouseLocation.endsWith("/")
            ? warehouseLocation.substring(0, warehouseLocation.length() - 1)
            : warehouseLocation;
    return this;
  }

  public VersionedUdfBuilder withVersionContext(ResolvedVersionContext versionContext) {
    this.versionContext = versionContext;
    return this;
  }

  public VersionedUdfBuilder withNessieClient(NessieClient nessieClient) {
    this.nessieClient = nessieClient;
    return this;
  }

  public VersionedUdfBuilder withUserName(String userName) {
    this.userName = userName;
    return this;
  }

  public VersionedUdfBuilder withFileIO(FileIO fileIO) {
    this.fileIO = fileIO;
    return this;
  }

  @Override
  public Udf create() {
    return create(newUdfOps().withCommitOrigin(IcebergCommitOrigin.CREATE_UDF));
  }

  @Override
  public Udf replace() {
    return replace(newUdfOps().withCommitOrigin(IcebergCommitOrigin.ALTER_UDF));
  }

  @Override
  public Udf createOrReplace() {
    VersionedUdfOperations ops = newUdfOps();
    if (null == ops.current()) {
      return create(ops.withCommitOrigin(IcebergCommitOrigin.CREATE_UDF));
    } else {
      return replace(ops.withCommitOrigin(IcebergCommitOrigin.ALTER_UDF));
    }
  }

  private VersionedUdfOperations newUdfOps() {
    return new VersionedUdfOperations(fileIO, nessieClient, udfKey, versionContext, userName);
  }

  private Udf create(UdfOperations ops) {
    if (null != ops.current()) {
      throw new AlreadyExistsException("UDF already exists: %s", Joiner.on('.').join(udfKey));
    }

    Preconditions.checkState(
        !representations.isEmpty(), "Cannot create UDF without specifying a body");
    Preconditions.checkState(null != signature, "Cannot create UDF without specifying signature");
    Preconditions.checkState(
        null != defaultNamespace, "Cannot create UDF without specifying a default namespace");

    UdfVersion udfVersion =
        ImmutableUdfVersion.builder()
            .versionId(UdfUtil.generateUUID())
            .signatureId(signature.signatureId())
            .addAllRepresentations(representations)
            .defaultNamespace(defaultNamespace)
            .defaultCatalog(defaultCatalog)
            .timestampMillis(System.currentTimeMillis())
            .putAllSummary(EnvironmentContext.get())
            .build();

    UdfMetadata udfMetadata =
        UdfMetadata.builder()
            .setProperties(properties)
            .setLocation(null != location ? location : defaultWarehouseLocation(udfKey))
            .setCurrentVersion(udfVersion, signature)
            .build();

    try {
      ops.commit(null, udfMetadata);
    } catch (CommitFailedException ignored) {
      throw new AlreadyExistsException("UDF was created concurrently: %s", udfKey);
    }

    return new BaseUdf(ops, ContentKey.of(udfKey).toString());
  }

  private Udf replace(UdfOperations ops) {
    if (null == ops.current()) {
      throw new NoSuchUdfException("UDF does not exist: %s", udfKey);
    }

    Preconditions.checkState(
        !representations.isEmpty(), "Cannot replace UDF without specifying a body");
    Preconditions.checkState(null != signature, "Cannot create UDF without specifying signature");
    Preconditions.checkState(
        null != defaultNamespace, "Cannot replace UDF without specifying a default namespace");

    UdfMetadata metadata = ops.current();
    UdfVersion udfVersion =
        ImmutableUdfVersion.builder()
            .versionId(UdfUtil.generateUUID())
            .signatureId(signature.signatureId())
            .addAllRepresentations(representations)
            .defaultNamespace(defaultNamespace)
            .defaultCatalog(defaultCatalog)
            .timestampMillis(System.currentTimeMillis())
            .putAllSummary(EnvironmentContext.get())
            .build();

    UdfMetadata.Builder builder =
        UdfMetadata.buildFrom(metadata)
            .setProperties(properties)
            .setCurrentVersion(udfVersion, signature);

    if (null != location) {
      builder.setLocation(location);
    }

    UdfMetadata replacement = builder.build();

    try {
      ops.commit(metadata, replacement);
    } catch (CommitFailedException ignored) {
      throw new AlreadyExistsException("UDF was updated concurrently: %s", udfKey);
    }

    return new BaseUdf(ops, ContentKey.of(udfKey).toString());
  }

  protected String defaultWarehouseLocation(List<String> viewKey) {
    return String.format("%s/%s", warehouseLocation, Joiner.on('/').join(viewKey));
  }

  private String getDialect(UdfMetadata udfMetadata) {
    SQLUdfRepresentation sqlUdfRepresentation =
        (SQLUdfRepresentation)
            udfMetadata
                .currentVersion()
                .representations()
                .get(udfMetadata.currentVersion().representations().size() - 1);
    return sqlUdfRepresentation.dialect();
  }
}
