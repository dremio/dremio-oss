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

import com.dremio.exec.store.iceberg.dremioudf.api.catalog.NoSuchUdfException;
import com.dremio.exec.store.iceberg.dremioudf.api.catalog.UdfCatalog;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.Udf;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfBuilder;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;

public abstract class BaseMetastoreUdfCatalog extends BaseMetastoreViewCatalog
    implements UdfCatalog {
  protected abstract UdfOperations newUdfOps(TableIdentifier identifier);

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
  }

  @Override
  public String name() {
    return super.name();
  }

  @Override
  public Udf loadUdf(TableIdentifier identifier) {
    if (isValidIdentifier(identifier)) {
      UdfOperations ops = newUdfOps(identifier);
      if (ops.current() == null) {
        throw new NoSuchUdfException("UDF does not exist: %s", identifier);
      } else {
        return new BaseUdf(newUdfOps(identifier), UdfUtil.fullUdfName(name(), identifier));
      }
    }

    throw new NoSuchUdfException("Invalid UDF identifier: %s", identifier);
  }

  @Override
  public UdfBuilder buildUdf(TableIdentifier identifier) {
    return new BaseUdfBuilder(identifier);
  }

  protected class BaseUdfBuilder implements UdfBuilder {
    private final TableIdentifier identifier;
    private final Map<String, String> properties = Maps.newHashMap();
    private final List<UdfRepresentation> representations = Lists.newArrayList();
    private Namespace defaultNamespace = null;
    private String defaultCatalog = null;
    private UdfSignature signature = null;
    private String location = null;

    protected BaseUdfBuilder(TableIdentifier identifier) {
      Preconditions.checkArgument(
          isValidIdentifier(identifier), "Invalid UDF identifier: %s", identifier);
      this.identifier = identifier;
    }

    @Override
    public UdfBuilder withSignature(UdfSignature newSignature) {
      this.signature = newSignature;
      return this;
    }

    @Override
    public UdfBuilder withBody(String dialect, String sql, String comment) {
      representations.add(
          ImmutableSQLUdfRepresentation.builder()
              .dialect(dialect)
              .body(sql)
              .comment(comment)
              .build());
      return this;
    }

    @Override
    public UdfBuilder withDefaultCatalog(String catalog) {
      this.defaultCatalog = catalog;
      return this;
    }

    @Override
    public UdfBuilder withDefaultNamespace(Namespace namespace) {
      this.defaultNamespace = namespace;
      return this;
    }

    @Override
    public UdfBuilder withProperties(Map<String, String> newProperties) {
      this.properties.putAll(newProperties);
      return this;
    }

    @Override
    public UdfBuilder withProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    @Override
    public UdfBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public Udf create() {
      return create(newUdfOps(identifier));
    }

    @Override
    public Udf replace() {
      return replace(newUdfOps(identifier));
    }

    @Override
    public Udf createOrReplace() {
      UdfOperations ops = newUdfOps(identifier);
      if (null == ops.current()) {
        return create(ops);
      } else {
        return replace(ops);
      }
    }

    private Udf create(UdfOperations ops) {
      if (null != ops.current()) {
        throw new AlreadyExistsException("UDF already exists: %s", identifier);
      }

      Preconditions.checkState(
          !representations.isEmpty(), "Cannot create UDF without specifying a body");
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
              .setLocation(null != location ? location : defaultWarehouseLocation(identifier))
              .setCurrentVersion(udfVersion, signature)
              .build();

      try {
        ops.commit(null, udfMetadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("UDF was created concurrently: %s", identifier);
      }

      return new BaseUdf(ops, UdfUtil.fullUdfName(name(), identifier));
    }

    private Udf replace(UdfOperations ops) {
      if (null == ops.current()) {
        throw new NoSuchUdfException("UDF does not exist: %s", identifier);
      }

      Preconditions.checkState(
          !representations.isEmpty(), "Cannot replace UDF without specifying a body");
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
        throw new AlreadyExistsException("UDF was updated concurrently: %s", identifier);
      }

      return new BaseUdf(ops, UdfUtil.fullUdfName(name(), identifier));
    }
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new BaseMetastoreUDFCatalogTableBuilder(identifier, schema);
  }

  /** The purpose of this class is to add UDF detection when replacing a table */
  protected class BaseMetastoreUDFCatalogTableBuilder extends BaseMetastoreCatalogTableBuilder {
    private final TableIdentifier identifier;

    public BaseMetastoreUDFCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
      this.identifier = identifier;
    }

    @Override
    public Table create() {
      if (udfExists(identifier)) {
        throw new AlreadyExistsException("UDF with same name already exists: %s", identifier);
      }

      return super.create();
    }

    @Override
    public Transaction replaceTransaction() {
      if (udfExists(identifier)) {
        throw new AlreadyExistsException("UDF with same name already exists: %s", identifier);
      }

      return super.replaceTransaction();
    }
  }
}
