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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.exec.store.iceberg.dremioudf.api.catalog.NoSuchUdfException;
import com.dremio.exec.store.iceberg.dremioudf.api.catalog.UdfCatalog;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.ReplaceUdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.Udf;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfBuilder;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfHistoryEntry;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UpdateUdfProperties;
import com.google.common.collect.ImmutableList;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class UdfCatalogTests<C extends UdfCatalog & SupportsNamespaces> {
  protected static final Schema SCHEMA =
      new Schema(
          5,
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  protected static final UdfSignature SIGNATURE =
      ImmutableUdfSignature.builder()
          .signatureId(UdfUtil.generateUUID())
          .addParameters(
              required(1, "x", Types.DoubleType.get(), "X coordinate"),
              required(2, "y", Types.DoubleType.get()))
          .returnType(Types.DoubleType.get())
          .deterministic(true)
          .build();

  protected static final UdfSignature OTHER_SIGNATURE =
      ImmutableUdfSignature.builder()
          .signatureId(UdfUtil.generateUUID())
          .addParameters(
              required(1, "x", Types.DoubleType.get(), "X coordinate"),
              required(2, "y", Types.DoubleType.get()),
              required(3, "z", Types.DoubleType.get()))
          .returnType(Types.DoubleType.get())
          .deterministic(true)
          .build();

  protected abstract C catalog();

  protected abstract Catalog tableCatalog();

  @TempDir private Path tempDir;

  protected boolean requiresNamespaceCreate() {
    return false;
  }

  protected boolean overridesRequestedLocation() {
    return false;
  }

  @Test
  public void basicCreateUDF() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("udf should not exist").isFalse();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("dremio", "sqrt(x * x, y * y)", "dremio comment")
            .create();

    assertThat(udf).isNotNull();
    assertThat(catalog().udfExists(identifier)).as("udf should exist").isTrue();
    assertThat(((BaseUdf) udf).operations().current().metadataFileLocation()).isNotNull();

    // validate udf settings
    assertThat(udf.name()).isEqualTo(UdfUtil.fullUdfName(catalog().name(), identifier));
    assertThat(udf.history()).hasSize(1);
    assertThat(udf.versions()).hasSize(1).containsExactly(udf.currentVersion());

    assertThat(udf.currentVersion())
        .isEqualTo(
            ImmutableUdfVersion.builder()
                .versionId(udf.currentVersion().versionId())
                .signatureId(udf.currentVersion().signatureId())
                .timestampMillis(udf.currentVersion().timestampMillis())
                .summary(udf.currentVersion().summary())
                .defaultNamespace(identifier.namespace())
                .addRepresentations(
                    ImmutableSQLUdfRepresentation.builder()
                        .body("sqrt(x * x, y * y)")
                        .dialect("dremio")
                        .comment("dremio comment")
                        .build())
                .build());

    assertThat(catalog().dropUdf(identifier)).isTrue();
    assertThat(catalog().udfExists(identifier)).as("udf should not exist").isFalse();
  }

  @Test
  public void completeCreateUDF() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    String location =
        Paths.get(tempDir.toUri().toString(), Paths.get("ns", "udf").toString()).toString();
    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withDefaultCatalog(catalog().name())
            .withBody("dremio", "select * from ns.tbl using X", "dremio comment")
            .withBody("spark", "select * from ns.tbl", "spark comment")
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2")
            .withLocation(location)
            .create();

    assertThat(udf).isNotNull();
    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();
    assertThat(((BaseUdf) udf).operations().current().metadataFileLocation()).isNotNull();

    if (!overridesRequestedLocation()) {
      assertThat(udf.location()).isEqualTo(location);
    } else {
      assertThat(udf.location()).isNotNull();
    }

    // validate udf settings
    assertThat(udf.uuid())
        .isEqualTo(UUID.fromString(((BaseUdf) udf).operations().current().uuid()));
    assertThat(udf.name()).isEqualTo(UdfUtil.fullUdfName(catalog().name(), identifier));
    assertThat(udf.properties()).containsEntry("prop1", "val1").containsEntry("prop2", "val2");
    assertThat(udf.history()).hasSize(1);
    assertThat(udf.versions()).hasSize(1).containsExactly(udf.currentVersion());

    assertThat(udf.currentVersion())
        .isEqualTo(
            ImmutableUdfVersion.builder()
                .versionId(udf.currentVersion().versionId())
                .signatureId(udf.currentVersion().signatureId())
                .timestampMillis(udf.currentVersion().timestampMillis())
                .summary(udf.currentVersion().summary())
                .defaultNamespace(identifier.namespace())
                .defaultCatalog(catalog().name())
                .addRepresentations(
                    ImmutableSQLUdfRepresentation.builder()
                        .body("select * from ns.tbl using X")
                        .dialect("dremio")
                        .comment("dremio comment")
                        .build())
                .addRepresentations(
                    ImmutableSQLUdfRepresentation.builder()
                        .body("select * from ns.tbl")
                        .dialect("spark")
                        .comment("spark comment")
                        .build())
                .build());

    assertThat(catalog().dropUdf(identifier)).isTrue();
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();
  }

  @Test
  public void createUDFErrorCases() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    SQLUdfRepresentation dremio =
        ImmutableSQLUdfRepresentation.builder()
            .body("select * from ns.tbl")
            .dialect("dremio")
            .comment("dremio comment")
            .build();

    // body is required
    assertThatThrownBy(() -> catalog().buildUdf(identifier).withSignature(SIGNATURE).create())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create UDF without specifying a body");

    // default namespace is required
    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(identifier)
                    .withSignature(SIGNATURE)
                    .withBody(dremio.dialect(), dremio.body(), dremio.comment())
                    .create())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create UDF without specifying a default namespace");

    // cannot define multiple SQLs for same dialect
    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(identifier)
                    .withSignature(SIGNATURE)
                    .withDefaultNamespace(identifier.namespace())
                    .withBody(dremio.dialect(), dremio.body(), dremio.comment())
                    .withBody(dremio.dialect(), dremio.body(), dremio.comment())
                    .create())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid UDF version: Cannot add multiple bodies for dialect dremio");
  }

  @Test
  public void createUDFThatAlreadyExists() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("spark", "select * from ns.tbl", "comment")
            .create();

    assertThat(udf).isNotNull();
    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(identifier)
                    .withSignature(SIGNATURE)
                    .withBody("spark", "select * from ns.tbl", "comment")
                    .withDefaultNamespace(identifier.namespace())
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("UDF already exists: ns.udf");
  }

  @Test
  public void createUDFThatAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(tableIdentifier)
                    .withSignature(SIGNATURE)
                    .withDefaultNamespace(tableIdentifier.namespace())
                    .withBody("spark", "select * from ns.tbl", "comment")
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table with same name already exists: ns.table");
  }

  @Test
  public void createTableThatAlreadyExistsAsUDF() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier udfIdentifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(udfIdentifier.namespace());
    }

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should not exist").isFalse();

    catalog()
        .buildUdf(udfIdentifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udfIdentifier.namespace())
        .withBody("spark", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should exist").isTrue();

    assertThatThrownBy(() -> tableCatalog().buildTable(udfIdentifier, SCHEMA).create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("UDF with same name already exists: ns.udf");
  }

  @Test
  public void createTableViaTransactionThatAlreadyExistsAsUDF() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier udfIdentifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(udfIdentifier.namespace());
    }

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should not exist").isFalse();

    Transaction transaction = tableCatalog().buildTable(udfIdentifier, SCHEMA).createTransaction();

    catalog()
        .buildUdf(udfIdentifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udfIdentifier.namespace())
        .withBody("spark", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should exist").isTrue();

    assertThatThrownBy(transaction::commitTransaction)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("UDF with same name already exists: ns.udf");
  }

  @Test
  public void replaceTableViaTransactionThatAlreadyExistsAsUDF() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier udfIdentifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(udfIdentifier.namespace());
    }

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should not exist").isFalse();

    catalog()
        .buildUdf(udfIdentifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udfIdentifier.namespace())
        .withBody("spark", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should exist").isTrue();

    // replace transaction requires table existence
    assertThatThrownBy(
            () ->
                tableCatalog()
                    .buildTable(udfIdentifier, SCHEMA)
                    .replaceTransaction()
                    .commitTransaction())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("UDF with same name already exists: ns.udf");
  }

  @Test
  public void createOrReplaceTableViaTransactionThatAlreadyExistsAsUDF() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier udfIdentifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(udfIdentifier.namespace());
    }

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should not exist").isFalse();

    catalog()
        .buildUdf(udfIdentifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udfIdentifier.namespace())
        .withBody("spark", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should exist").isTrue();

    assertThatThrownBy(
            () ->
                tableCatalog()
                    .buildTable(udfIdentifier, SCHEMA)
                    .createOrReplaceTransaction()
                    .commitTransaction())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("UDF with same name already exists: ns.udf");
  }

  @Test
  public void replaceUDFThatAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // replace udf requires the udf to exist
    // TODO: replace should check whether the udf exists as a table
    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(tableIdentifier)
                    .withSignature(SIGNATURE)
                    .withDefaultNamespace(tableIdentifier.namespace())
                    .withBody("spark", "select * from ns.tbl", "comment")
                    .replace())
        .isInstanceOf(NoSuchUdfException.class)
        .hasMessageStartingWith("UDF does not exist: ns.table");
  }

  @Test
  public void createOrReplaceUDFThatAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(tableIdentifier)
                    .withSignature(SIGNATURE)
                    .withDefaultNamespace(tableIdentifier.namespace())
                    .withBody("spark", "select * from ns.tbl", "comment")
                    .createOrReplace())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table with same name already exists: ns.table");
  }

  @Test
  public void renameUDF() {
    TableIdentifier from = TableIdentifier.of("ns", "udf");
    TableIdentifier to = TableIdentifier.of("ns", "renamedUDF");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    assertThat(catalog().udfExists(from)).as("UDF should not exist").isFalse();

    Udf udf =
        catalog()
            .buildUdf(from)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(from.namespace())
            .withBody("spark", "select * from ns.tbl", "comment")
            .create();

    assertThat(catalog().udfExists(from)).as("UDF should exist").isTrue();

    UdfMetadata original = ((BaseUdf) udf).operations().current();
    assertThat(original.metadataFileLocation()).isNotNull();

    catalog().renameUdf(from, to);

    assertThat(catalog().udfExists(from)).as("UDF should not exist with old name").isFalse();
    assertThat(catalog().udfExists(to)).as("UDF should exist with new name").isTrue();

    // ensure udf metadata didn't change after renaming
    Udf renamed = catalog().loadUdf(to);
    assertThat(((BaseUdf) renamed).operations().current())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Schema.class)
        .isEqualTo(original);

    assertThat(catalog().dropUdf(from)).isFalse();
    assertThat(catalog().dropUdf(to)).isTrue();
    assertThat(catalog().udfExists(to)).as("UDF should not exist").isFalse();
  }

  @Test
  public void renameUDFUsingDifferentNamespace() {
    TableIdentifier from = TableIdentifier.of("ns", "udf");
    TableIdentifier to = TableIdentifier.of("other_ns", "renamedUDF");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
      catalog().createNamespace(to.namespace());
    }

    assertThat(catalog().udfExists(from)).as("udf should not exist").isFalse();

    Udf udf =
        catalog()
            .buildUdf(from)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(from.namespace())
            .withBody("spark", "select * from ns.tbl", "comment")
            .create();

    assertThat(catalog().udfExists(from)).as("udf should exist").isTrue();

    UdfMetadata original = ((BaseUdf) udf).operations().current();

    catalog().renameUdf(from, to);

    assertThat(catalog().udfExists(from)).as("udf should not exist with old name").isFalse();
    assertThat(catalog().udfExists(to)).as("udf should exist with new name").isTrue();

    // ensure udf metadata didn't change after renaming
    Udf renamed = catalog().loadUdf(to);
    assertThat(((BaseUdf) renamed).operations().current())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Schema.class)
        .isEqualTo(original);

    assertThat(catalog().dropUdf(from)).isFalse();
    assertThat(catalog().dropUdf(to)).isTrue();
    assertThat(catalog().udfExists(to)).as("udf should not exist").isFalse();
  }

  @Test
  public void renameUDFNamespaceMissing() {
    TableIdentifier from = TableIdentifier.of("ns", "udf");
    TableIdentifier to = TableIdentifier.of("non_existing", "renamedUDF");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    assertThat(catalog().udfExists(from)).as("UDF should not exist").isFalse();

    catalog()
        .buildUdf(from)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(from.namespace())
        .withBody("spark", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(from)).as("UDF should exist").isTrue();

    assertThatThrownBy(() -> catalog().renameUdf(from, to))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist: non_existing");
  }

  @Test
  public void renameUDFSourceMissing() {
    TableIdentifier from = TableIdentifier.of("ns", "non_existing");
    TableIdentifier to = TableIdentifier.of("ns", "renamedUDF");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(from.namespace());
    }

    assertThat(catalog().udfExists(from)).as("UDF should not exist").isFalse();

    assertThatThrownBy(() -> catalog().renameUdf(from, to))
        .isInstanceOf(NoSuchUdfException.class)
        .hasMessageContaining("UDF does not exist");
  }

  @Test
  public void renameUDFTargetAlreadyExistsAsUDF() {
    TableIdentifier udfOne = TableIdentifier.of("ns", "udfOne");
    TableIdentifier udfTwo = TableIdentifier.of("ns", "udfTwo");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(udfOne.namespace());
    }

    for (TableIdentifier identifier : ImmutableList.of(udfOne, udfTwo)) {
      assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

      catalog()
          .buildUdf(identifier)
          .withSignature(SIGNATURE)
          .withDefaultNamespace(udfOne.namespace())
          .withBody("spark", "select * from ns.tbl", "comment")
          .create();

      assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();
    }

    assertThatThrownBy(() -> catalog().renameUdf(udfOne, udfTwo))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Cannot rename ns.udfOne to ns.udfTwo. UDF already exists");
  }

  @Test
  public void renameUDFTargetAlreadyExistsAsTable() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier udfIdentifier = TableIdentifier.of("ns", "udf");
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should not exist").isFalse();

    catalog()
        .buildUdf(udfIdentifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udfIdentifier.namespace())
        .withBody("spark", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should exist").isTrue();

    assertThatThrownBy(() -> catalog().renameUdf(udfIdentifier, tableIdentifier))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Cannot rename ns.udf to ns.table. Table already exists");
  }

  @Test
  public void renameTableTargetAlreadyExistsAsUDF() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    TableIdentifier udfIdentifier = TableIdentifier.of("ns", "udf");
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should not exist").isFalse();

    catalog()
        .buildUdf(udfIdentifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udfIdentifier.namespace())
        .withBody("spark", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(udfIdentifier)).as("UDF should exist").isTrue();

    assertThatThrownBy(() -> tableCatalog().renameTable(tableIdentifier, udfIdentifier))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Cannot rename ns.table to ns.udf. UDF already exists");
  }

  @Test
  public void listUDFs() {
    Namespace ns1 = Namespace.of("ns1");
    Namespace ns2 = Namespace.of("ns2");

    TableIdentifier udf1 = TableIdentifier.of(ns1, "udf1");
    TableIdentifier udf2 = TableIdentifier.of(ns2, "udf2");
    TableIdentifier udf3 = TableIdentifier.of(ns2, "udf3");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(ns1);
      catalog().createNamespace(ns2);
    }

    assertThat(catalog().listUdfs(ns1)).isEmpty();
    assertThat(catalog().listUdfs(ns2)).isEmpty();

    catalog()
        .buildUdf(udf1)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udf1.namespace())
        .withBody("spark", "select * from ns1.tbl", "spark comment")
        .create();

    assertThat(catalog().listUdfs(ns1)).containsExactly(udf1);
    assertThat(catalog().listUdfs(ns2)).isEmpty();

    catalog()
        .buildUdf(udf2)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udf2.namespace())
        .withBody("spark", "select * from ns1.tbl", "spark comment")
        .create();

    assertThat(catalog().listUdfs(ns1)).containsExactly(udf1);
    assertThat(catalog().listUdfs(ns2)).containsExactly(udf2);

    catalog()
        .buildUdf(udf3)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udf3.namespace())
        .withBody("spark", "select * from ns.tbl", "spark comment")
        .create();

    assertThat(catalog().listUdfs(ns1)).containsExactly(udf1);
    assertThat(catalog().listUdfs(ns2)).containsExactlyInAnyOrder(udf2, udf3);

    assertThat(catalog().dropUdf(udf2)).isTrue();
    assertThat(catalog().listUdfs(ns1)).containsExactly(udf1);
    assertThat(catalog().listUdfs(ns2)).containsExactly(udf3);

    assertThat(catalog().dropUdf(udf3)).isTrue();
    assertThat(catalog().listUdfs(ns1)).containsExactly(udf1);
    assertThat(catalog().listUdfs(ns2)).isEmpty();

    assertThat(catalog().dropUdf(udf1)).isTrue();
    assertThat(catalog().listUdfs(ns1)).isEmpty();
    assertThat(catalog().listUdfs(ns2)).isEmpty();
  }

  @Test
  public void listUDFsAndTables() {
    Assumptions.assumeThat(tableCatalog())
        .as("Only valid for catalogs that support tables")
        .isNotNull();

    Namespace ns = Namespace.of("ns");

    TableIdentifier tableIdentifier = TableIdentifier.of(ns, "table");
    TableIdentifier udfIdentifier = TableIdentifier.of(ns, "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(ns);
    }

    assertThat(catalog().listUdfs(ns)).isEmpty();
    assertThat(tableCatalog().listTables(ns)).isEmpty();

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(catalog().listUdfs(ns)).isEmpty();
    assertThat(tableCatalog().listTables(ns)).containsExactly(tableIdentifier);

    catalog()
        .buildUdf(udfIdentifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(udfIdentifier.namespace())
        .withBody("spark", "select * from ns1.tbl", "spark comment")
        .create();

    assertThat(catalog().listUdfs(ns)).containsExactly(udfIdentifier);
    assertThat(tableCatalog().listTables(ns)).containsExactly(tableIdentifier);

    assertThat(tableCatalog().dropTable(tableIdentifier)).isTrue();
    assertThat(catalog().listUdfs(ns)).containsExactly(udfIdentifier);
    assertThat(tableCatalog().listTables(ns)).isEmpty();

    assertThat(catalog().dropUdf(udfIdentifier)).isTrue();
    assertThat(catalog().listUdfs(ns)).isEmpty();
    assertThat(tableCatalog().listTables(ns)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void createOrReplaceUDF(boolean useCreateOrReplace) {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    UdfBuilder udfBuilder =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("spark", "select * from ns.tbl", "spark comment")
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2");
    Udf udf = useCreateOrReplace ? udfBuilder.createOrReplace() : udfBuilder.create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();
    assertThat(((BaseUdf) udf).operations().current().metadataFileLocation()).isNotNull();

    UdfVersion udfVersion = udf.currentVersion();
    assertThat(udfVersion.representations())
        .containsExactly(
            ImmutableSQLUdfRepresentation.builder()
                .body("select * from ns.tbl")
                .dialect("spark")
                .comment("spark comment")
                .build());

    udfBuilder =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("trino", "select count(*) from ns.tbl", "trino comment")
            .withProperty("replacedProp1", "val1")
            .withProperty("replacedProp2", "val2");
    Udf replacedUdf = useCreateOrReplace ? udfBuilder.createOrReplace() : udfBuilder.replace();

    // validate replaced udf settings
    assertThat(replacedUdf.name()).isEqualTo(UdfUtil.fullUdfName(catalog().name(), identifier));
    assertThat(((BaseUdf) replacedUdf).operations().current().metadataFileLocation()).isNotNull();
    assertThat(replacedUdf.properties())
        .containsEntry("prop1", "val1")
        .containsEntry("prop2", "val2")
        .containsEntry("replacedProp1", "val1")
        .containsEntry("replacedProp2", "val2");
    assertThat(replacedUdf.history()).hasSize(2);

    UdfVersion replacedUdfVersion = replacedUdf.currentVersion();
    assertThat(replacedUdf.versions()).hasSize(2).containsExactly(udfVersion, replacedUdfVersion);
    assertThat(replacedUdfVersion).isNotNull();
    assertThat(replacedUdfVersion.representations())
        .containsExactly(
            ImmutableSQLUdfRepresentation.builder()
                .body("select count(*) from ns.tbl")
                .dialect("trino")
                .comment("trino comment")
                .build());

    assertThat(catalog().dropUdf(identifier)).isTrue();
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();
  }

  @Test
  public void replaceUDFErrorCases() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    SQLUdfRepresentation trino =
        ImmutableSQLUdfRepresentation.builder()
            .body("select * from ns.tbl")
            .dialect("trino")
            .comment("trino comment")
            .build();

    catalog()
        .buildUdf(identifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(identifier.namespace())
        .withBody(trino.dialect(), trino.body(), trino.comment())
        .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    // body is required
    assertThatThrownBy(() -> catalog().buildUdf(identifier).withSignature(SIGNATURE).replace())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace UDF without specifying a body");

    // default namespace is required
    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(identifier)
                    .withSignature(SIGNATURE)
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .replace())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace UDF without specifying a default namespace");

    // cannot replace non-existing udf
    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(TableIdentifier.of("ns", "non_existing"))
                    .withSignature(SIGNATURE)
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .withDefaultNamespace(identifier.namespace())
                    .replace())
        .isInstanceOf(NoSuchUdfException.class)
        .hasMessageStartingWith("UDF does not exist: ns.non_existing");

    // cannot define multiple SQLs for same dialect
    assertThatThrownBy(
            () ->
                catalog()
                    .buildUdf(identifier)
                    .withSignature(SIGNATURE)
                    .withDefaultNamespace(identifier.namespace())
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .replace())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid UDF version: Cannot add multiple bodies for dialect trino");
  }

  @Test
  public void updateUDFProperties() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("spark", "select * from ns.tbl", "comment")
            .create();

    UdfVersion udfVersion = udf.currentVersion();

    udf.updateProperties().set("key1", "val1").set("key2", "val2").remove("non-existing").commit();

    Udf updatedUdf = catalog().loadUdf(identifier);
    assertThat(updatedUdf.properties()).containsEntry("key1", "val1").containsEntry("key2", "val2");

    // history and udf versions should stay the same after updating udf properties
    assertThat(updatedUdf.history()).hasSize(1).isEqualTo(udf.history());
    assertThat(updatedUdf.versions()).hasSize(1).containsExactly(udfVersion);

    assertThat(catalog().dropUdf(identifier)).isTrue();
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();
  }

  @Test
  public void updateUDFPropertiesErrorCases() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    catalog()
        .buildUdf(identifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(identifier.namespace())
        .withBody("spark", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    assertThatThrownBy(
            () -> catalog().loadUdf(identifier).updateProperties().set(null, "new-val1").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key: null");

    assertThatThrownBy(
            () -> catalog().loadUdf(identifier).updateProperties().set("key1", null).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid value: null");

    assertThatThrownBy(() -> catalog().loadUdf(identifier).updateProperties().remove(null).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key: null");

    assertThatThrownBy(
            () ->
                catalog()
                    .loadUdf(identifier)
                    .updateProperties()
                    .set("key1", "x")
                    .set("key3", "y")
                    .remove("key2")
                    .set("key2", "z")
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove and update the same key: key2");
  }

  @Test
  public void replaceUDFVersion() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    SQLUdfRepresentation spark =
        ImmutableSQLUdfRepresentation.builder()
            .dialect("spark")
            .body("select * from ns.tbl")
            .comment("spark comment")
            .build();

    SQLUdfRepresentation trino =
        ImmutableSQLUdfRepresentation.builder()
            .body("select * from ns.tbl")
            .dialect("trino")
            .comment("trino comment")
            .build();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody(trino.dialect(), trino.body(), trino.comment())
            .withBody(spark.dialect(), spark.body(), spark.comment())
            .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    UdfVersion udfVersion = udf.currentVersion();
    assertThat(udfVersion.representations()).hasSize(2).containsExactly(trino, spark);

    // uses a different signature and udf representation
    udf.replaceVersion()
        .withSignature(OTHER_SIGNATURE)
        .withBody(trino.dialect(), trino.body(), trino.comment())
        .withDefaultCatalog("default")
        .withDefaultNamespace(identifier.namespace())
        .commit();

    // history and udf versions should reflect the changes
    Udf updatedUdf = catalog().loadUdf(identifier);
    assertThat(updatedUdf.history())
        .hasSize(2)
        .element(0)
        .extracting(UdfHistoryEntry::versionId)
        .isEqualTo(udfVersion.versionId());
    assertThat(updatedUdf.history())
        .element(1)
        .extracting(UdfHistoryEntry::versionId)
        .isEqualTo(updatedUdf.currentVersion().versionId());
    assertThat(updatedUdf.versions())
        .hasSize(2)
        .containsExactly(udfVersion, updatedUdf.currentVersion());

    UdfVersion updatedUdfVersion = updatedUdf.currentVersion();
    assertThat(updatedUdfVersion).isNotNull();
    assertThat(updatedUdfVersion.representations()).hasSize(1).containsExactly(trino);
    assertThat(updatedUdfVersion.defaultCatalog()).isEqualTo("default");
    assertThat(updatedUdfVersion.defaultNamespace()).isEqualTo(identifier.namespace());

    assertThat(catalog().dropUdf(identifier)).isTrue();
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();
  }

  @Test
  public void replaceUDFVersionByUpdatingSQLForDialect() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    SQLUdfRepresentation spark =
        ImmutableSQLUdfRepresentation.builder()
            .body("select * from ns.tbl")
            .dialect("spark")
            .comment("spark comment")
            .build();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody(spark.dialect(), spark.body(), spark.comment())
            .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    UdfVersion udfVersion = udf.currentVersion();
    assertThat(udfVersion.representations()).hasSize(1).containsExactly(spark);

    SQLUdfRepresentation updatedSpark =
        ImmutableSQLUdfRepresentation.builder()
            .body("select * from ns.updated_tbl")
            .dialect("spark")
            .comment("updated comment")
            .build();

    // only update the SQL for spark
    udf.replaceVersion()
        .withSignature(SIGNATURE)
        .withDefaultNamespace(identifier.namespace())
        .withBody(updatedSpark.dialect(), updatedSpark.body(), updatedSpark.comment())
        .commit();

    // history and udf versions should reflect the changes
    Udf updatedUdf = catalog().loadUdf(identifier);
    assertThat(updatedUdf.history())
        .hasSize(2)
        .element(0)
        .extracting(UdfHistoryEntry::versionId)
        .isEqualTo(udfVersion.versionId());
    assertThat(updatedUdf.history())
        .element(1)
        .extracting(UdfHistoryEntry::versionId)
        .isEqualTo(updatedUdf.currentVersion().versionId());
    assertThat(updatedUdf.versions())
        .hasSize(2)
        .containsExactly(udfVersion, updatedUdf.currentVersion());

    // updated udf should have the new SQL
    assertThat(updatedUdf.currentVersion().representations())
        .hasSize(1)
        .containsExactly(updatedSpark);

    assertThat(catalog().dropUdf(identifier)).isTrue();
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();
  }

  @Test
  public void replaceUDFVersionErrorCases() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    SQLUdfRepresentation trino =
        ImmutableSQLUdfRepresentation.builder()
            .body("select * from ns.tbl")
            .dialect("trino")
            .comment("trino comment")
            .build();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody(trino.dialect(), trino.body(), trino.comment())
            .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    // empty commits are not allowed
    assertThatThrownBy(() -> udf.replaceVersion().commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace UDF without specifying a body");

    // schema is required
    Assertions.assertThatThrownBy(
            () ->
                udf.replaceVersion()
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .withDefaultNamespace(identifier.namespace())
                    .commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace UDF without specifying signature");

    // default namespace is required
    assertThatThrownBy(
            () ->
                udf.replaceVersion()
                    .withSignature(SIGNATURE)
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot replace UDF without specifying a default namespace");

    // cannot define multiple SQLs for same dialect
    assertThatThrownBy(
            () ->
                udf.replaceVersion()
                    .withSignature(SIGNATURE)
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .withDefaultNamespace(identifier.namespace())
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .withBody(trino.dialect(), trino.body(), trino.comment())
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid UDF version: Cannot add multiple bodies for dialect trino");
  }

  @Test
  public void updateUDFPropertiesConflict() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("trino", "select * from ns.tbl", "comment")
            .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();
    UpdateUdfProperties updateUDFProperties = udf.updateProperties();

    // drop udf and then try to use the updateProperties API
    catalog().dropUdf(identifier);
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    assertThatThrownBy(() -> updateUDFProperties.set("key1", "val1").commit())
        .isInstanceOf(NoSuchUdfException.class)
        .hasMessageContaining("UDF does not exist: ns.udf");
  }

  @Test
  public void replaceUDFVersionConflict() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("trino", "select * from ns.tbl", "comment")
            .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();
    ReplaceUdfVersion replaceUDFVersion = udf.replaceVersion();

    // drop udf and then try to use the replaceVersion API
    catalog().dropUdf(identifier);
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    assertThatThrownBy(
            () ->
                replaceUDFVersion
                    .withSignature(SIGNATURE)
                    .withBody("trino", "select * from ns.tbl", "comment")
                    .withDefaultNamespace(identifier.namespace())
                    .commit())
        .isInstanceOf(NoSuchUdfException.class)
        .hasMessageContaining("UDF does not exist: ns.udf");
  }

  @Test
  public void createUDFConflict() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();
    UdfBuilder udfBuilder = catalog().buildUdf(identifier);

    catalog()
        .buildUdf(identifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(identifier.namespace())
        .withBody("trino", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    // the udf was already created concurrently
    assertThatThrownBy(
            () ->
                udfBuilder
                    .withSignature(SIGNATURE)
                    .withBody("trino", "select * from ns.tbl", "comment")
                    .withDefaultNamespace(identifier.namespace())
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("UDF already exists: ns.udf");
  }

  @Test
  public void replaceUDFConflict() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    catalog()
        .buildUdf(identifier)
        .withSignature(SIGNATURE)
        .withDefaultNamespace(identifier.namespace())
        .withBody("trino", "select * from ns.tbl", "comment")
        .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();
    UdfBuilder udfBuilder = catalog().buildUdf(identifier);

    catalog().dropUdf(identifier);
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    // the udf was already dropped concurrently
    assertThatThrownBy(
            () ->
                udfBuilder
                    .withSignature(SIGNATURE)
                    .withBody("trino", "select * from ns.tbl", "comment")
                    .withDefaultNamespace(identifier.namespace())
                    .replace())
        .isInstanceOf(NoSuchUdfException.class)
        .hasMessageStartingWith("UDF does not exist: ns.udf");
  }

  @Test
  public void createAndReplaceUDFWithLocation() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    String location =
        Paths.get(tempDir.toUri().toString(), Paths.get("ns", "udf").toString()).toString();
    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("trino", "select * from ns.tbl", "comment")
            .withLocation(location)
            .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    if (!overridesRequestedLocation()) {
      assertThat(udf.location()).isEqualTo(location);
    } else {
      assertThat(udf.location()).isNotNull();
    }

    String updatedLocation =
        Paths.get(tempDir.toUri().toString(), Paths.get("updated", "ns", "udf").toString())
            .toString();
    udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("trino", "select * from ns.tbl", "comment")
            .withLocation(updatedLocation)
            .replace();

    if (!overridesRequestedLocation()) {
      assertThat(udf.location()).isEqualTo(updatedLocation);
    } else {
      assertThat(udf.location()).isNotNull();
    }

    assertThat(catalog().dropUdf(identifier)).isTrue();
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();
  }

  @Test
  public void updateUDFLocation() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    String location =
        Paths.get(tempDir.toUri().toString(), Paths.get("ns", "udf").toString()).toString();
    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("trino", "select * from ns.tbl", "comment")
            .withLocation(location)
            .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();
    if (!overridesRequestedLocation()) {
      assertThat(udf.location()).isEqualTo(location);
    } else {
      assertThat(udf.location()).isNotNull();
    }

    String updatedLocation =
        Paths.get(tempDir.toUri().toString(), Paths.get("updated", "ns", "udf").toString())
            .toString();
    udf.updateLocation().setLocation(updatedLocation).commit();

    Udf updatedUdf = catalog().loadUdf(identifier);

    if (!overridesRequestedLocation()) {
      assertThat(updatedUdf.location()).isEqualTo(updatedLocation);
    } else {
      assertThat(udf.location()).isNotNull();
    }

    // history and udf versions should stay the same after updating udf properties
    assertThat(updatedUdf.history()).hasSize(1).isEqualTo(udf.history());
    assertThat(updatedUdf.versions()).hasSize(1).containsExactly(udf.currentVersion());

    assertThat(catalog().dropUdf(identifier)).isTrue();
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();
  }

  @Test
  public void updateUDFLocationConflict() {
    TableIdentifier identifier = TableIdentifier.of("ns", "udf");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(identifier.namespace());
    }

    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    Udf udf =
        catalog()
            .buildUdf(identifier)
            .withSignature(SIGNATURE)
            .withDefaultNamespace(identifier.namespace())
            .withBody("trino", "select * from ns.tbl", "comment")
            .create();

    assertThat(catalog().udfExists(identifier)).as("UDF should exist").isTrue();

    // new location must be non-null
    assertThatThrownBy(() -> udf.updateLocation().setLocation(null).commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid UDF location: null");

    UpdateLocation updateUDFLocation = udf.updateLocation();

    catalog().dropUdf(identifier);
    assertThat(catalog().udfExists(identifier)).as("UDF should not exist").isFalse();

    // the udf was already dropped concurrently
    assertThatThrownBy(() -> updateUDFLocation.setLocation("new-location").commit())
        .isInstanceOf(NoSuchUdfException.class)
        .hasMessageContaining("UDF does not exist: ns.udf");
  }
}
