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
package com.dremio.plugins.icebergcatalog.store;

import static com.dremio.exec.catalog.CatalogOptions.RESTCATALOG_VIEWS_SUPPORTED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS;
import static com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_PLUGIN_VIEW_CACHE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.options.OptionManager;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import javax.net.ssl.SSLHandshakeException;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ViewBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Error mapping of namespace (folder), table and view operations of {@link
 * AbstractRestCatalogAccessor} against a mocked Iceberg REST catalog. The server messages used here
 * are the ones Apache Polaris 1.1 returns.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestRestCatalogNamespaceTableOps {

  private static final String SOURCE = "polaris";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final String POLARIS_PURGE_403 =
      "Forbidden: Unable to purge entity: v1. To enable this feature, set the Polaris"
          + " configuration DROP_WITH_PURGE_ENABLED or the catalog configuration"
          + " polaris.config.drop-with-purge.enabled";
  private static final String POLARIS_GRANT_403 =
      "Forbidden: Principal 'reader' with activated PrincipalRoles '[]' and activated grants via"
          + " '[reader_pr, reader_cr]' is not authorized for op %s";

  @Mock private RESTCatalog catalog;
  @Mock private OptionManager optionManager;

  private AbstractRestCatalogAccessor accessor;

  @Before
  public void setup() {
    lenient()
        .when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_SIZE_ITEMS))
        .thenReturn(10L);
    lenient()
        .when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_EXPIRE_AFTER_WRITE_SECONDS))
        .thenReturn(10L);
    lenient().when(optionManager.getOption(RESTCATALOG_VIEWS_SUPPORTED)).thenReturn(true);
    Supplier<Catalog> supplier = () -> catalog;
    accessor = new IcebergRestCatalogAccessor(supplier, optionManager, null, false);
  }

  private static List<String> path(String... components) {
    List<String> path = new ArrayList<>();
    path.add(SOURCE);
    Collections.addAll(path, components);
    return path;
  }

  private static void assertUserException(Throwable t, ErrorType type, String... fragments) {
    assertThat(t).isInstanceOf(UserException.class);
    UserException ue = (UserException) t;
    assertThat(ue.getErrorType()).isEqualTo(type);
    for (String fragment : fragments) {
      assertThat(ue.getOriginalMessage()).contains(fragment);
    }
  }

  // --- namespaces (folders) ---

  @Test
  public void testDropFolderNotEmptyBadRequestIsNamespaceNotEmpty() {
    // Apache Polaris 1.x answers HTTP 400: reported like the spec's HTTP 409, so that
    // RestIcebergCatalogPlugin#deleteFolder handles one exception type.
    when(catalog.dropNamespace(Namespace.of("ns1")))
        .thenThrow(new BadRequestException("Malformed request: Namespace ns1 is not empty"));

    assertThatThrownBy(() -> accessor.dropFolder(path("ns1")))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessageContaining("ns1")
        .hasCauseInstanceOf(BadRequestException.class);
  }

  @Test
  public void testDropFolderNotEmptyConflictIsPassedThrough() {
    NamespaceNotEmptyException conflict =
        new NamespaceNotEmptyException("Namespace a.b is not empty");
    when(catalog.dropNamespace(Namespace.of("a", "b"))).thenThrow(conflict);

    assertThatThrownBy(() -> accessor.dropFolder(path("a", "b"))).isSameAs(conflict);
  }

  @Test
  public void testNamespaceNotEmptyIsValidationError() {
    UserException e =
        RestCatalogExceptionMapper.namespaceNotEmpty(
            new NamespaceNotEmptyException("Namespace a.b is not empty"), Namespace.of("a", "b"));

    assertUserException(e, ErrorType.VALIDATION, "Folder [a.b]", "not empty", "Drop the tables");
  }

  @Test
  public void testDropFolderOtherBadRequestIsValidationError() {
    when(catalog.dropNamespace(Namespace.of("ns1")))
        .thenThrow(new BadRequestException("Malformed request: something else"));

    assertThatThrownBy(() -> accessor.dropFolder(path("ns1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.VALIDATION,
                    "rejected the request to drop folder [ns1] as invalid (HTTP 400)",
                    "something else"));
  }

  @Test
  public void testDropFolderMissingThrowsNoSuchNamespace() {
    when(catalog.dropNamespace(Namespace.of("nope"))).thenReturn(false);

    assertThatThrownBy(() -> accessor.dropFolder(path("nope")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("nope");
  }

  @Test
  public void testDropFolderEmptySucceeds() {
    when(catalog.dropNamespace(Namespace.of("a", "b", "c"))).thenReturn(true);

    assertThat(accessor.dropFolder(path("a", "b", "c"))).isTrue();
  }

  @Test
  public void testDropFolderForbiddenIsPermissionError() {
    when(catalog.dropNamespace(Namespace.of("ns1")))
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "DROP_NAMESPACE"));

    assertThatThrownBy(() -> accessor.dropFolder(path("ns1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.PERMISSION,
                    "drop folder [ns1]",
                    "DROP_NAMESPACE",
                    RestCatalogExceptionMapper.MISSING_PRIVILEGE_HINT));
  }

  @Test
  public void testCreateFolderForbiddenIsPermissionError() {
    doThrow(new ForbiddenException(POLARIS_GRANT_403, "CREATE_NAMESPACE"))
        .when(catalog)
        .createNamespace(eq(Namespace.of("a", "b")), anyMap());

    assertThatThrownBy(() -> accessor.createFolder(path("a", "b"), Collections.emptyMap()))
        .satisfies(
            t -> assertUserException(t, ErrorType.PERMISSION, "create folder [a.b]", "CREATE_"));
  }

  @Test
  public void testCreateFolderMissingParentIsValidationError() {
    doThrow(new NoSuchNamespaceException("Namespace does not exist: a"))
        .when(catalog)
        .createNamespace(eq(Namespace.of("a", "b", "c")), anyMap());

    assertThatThrownBy(() -> accessor.createFolder(path("a", "b", "c"), Collections.emptyMap()))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.VALIDATION,
                    "Cannot create folder [a.b.c]",
                    "parent folder [a.b] does not exist"));
  }

  @Test
  public void testCreateFolderAlreadyExistsIsPassedThrough() {
    // RestIcebergCatalogPlugin#createFolder maps it to CatalogEntityAlreadyExistsException.
    doThrow(new AlreadyExistsException("Namespace already exists: ns1"))
        .when(catalog)
        .createNamespace(eq(Namespace.of("ns1")), anyMap());

    assertThatThrownBy(() -> accessor.createFolder(path("ns1"), Collections.emptyMap()))
        .isInstanceOf(AlreadyExistsException.class);
  }

  // --- views ---

  private static ViewBuilder mockViewBuilder() {
    // ViewBuilder's fluent methods are generic, so RETURNS_SELF does not apply to them.
    return mock(
        ViewBuilder.class,
        invocation ->
            invocation.getMethod().getReturnType().isInstance(invocation.getMock())
                ? invocation.getMock()
                : null);
  }

  @Test
  public void testDropViewPurgeForbiddenHasPurgeHint() {
    doThrow(new ForbiddenException(POLARIS_PURGE_403))
        .when(catalog)
        .dropView(TableIdentifier.of("ns1", "v1"));

    assertThatThrownBy(() -> accessor.dropView(path("ns1", "v1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.PERMISSION,
                    "drop view [ns1.v1]",
                    "Unable to purge entity",
                    RestCatalogExceptionMapper.DROP_WITH_PURGE_HINT))
        .hasCauseInstanceOf(ForbiddenException.class);
  }

  @Test
  public void testDropViewGrantForbiddenHasPrivilegeHint() {
    doThrow(new ForbiddenException(POLARIS_GRANT_403, "DROP_VIEW"))
        .when(catalog)
        .dropView(TableIdentifier.of("ns1", "v1"));

    assertThatThrownBy(() -> accessor.dropView(path("ns1", "v1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.PERMISSION,
                    "DROP_VIEW",
                    RestCatalogExceptionMapper.MISSING_PRIVILEGE_HINT));
  }

  @Test
  public void testDropViewMissingIsPassedThrough() {
    // RestIcebergCatalogPlugin#dropView maps it to CatalogEntityNotFoundException.
    doThrow(new NoSuchViewException("View does not exist: ns1.v1"))
        .when(catalog)
        .dropView(TableIdentifier.of("ns1", "v1"));

    assertThatThrownBy(() -> accessor.dropView(path("ns1", "v1")))
        .isInstanceOf(NoSuchViewException.class);
  }

  @Test
  public void testCreateViewForbiddenIsPermissionError() {
    TableIdentifier id = TableIdentifier.of("ns1", "v1");
    when(catalog.viewExists(id)).thenReturn(false);
    ViewBuilder builder = mockViewBuilder();
    when(catalog.buildView(id)).thenReturn(builder);
    when(builder.create()).thenThrow(new ForbiddenException(POLARIS_GRANT_403, "CREATE_VIEW"));

    assertThatThrownBy(
            () ->
                accessor.createView(
                    path("ns1", "v1"), "s3://b/ns1/v1", List.of(SOURCE), SCHEMA, "SELECT 1"))
        .satisfies(
            t ->
                assertUserException(
                    t, ErrorType.PERMISSION, "create view [ns1.v1]", "CREATE_VIEW"));
  }

  @Test
  public void testUpdateViewForbiddenIsPermissionError() {
    TableIdentifier id = TableIdentifier.of("ns1", "v1");
    when(catalog.viewExists(id)).thenReturn(true);
    ViewBuilder builder = mockViewBuilder();
    when(catalog.buildView(id)).thenReturn(builder);
    when(builder.replace()).thenThrow(new ForbiddenException(POLARIS_GRANT_403, "REPLACE_VIEW"));

    assertThatThrownBy(
            () ->
                accessor.updateView(
                    path("ns1", "v1"), "s3://b/ns1/v1", List.of(SOURCE), SCHEMA, "SELECT 2"))
        .satisfies(
            t ->
                assertUserException(
                    t, ErrorType.PERMISSION, "replace view [ns1.v1]", "REPLACE_VIEW"));
  }

  @Test
  public void testLoadViewForbiddenIsPermissionError() {
    when(optionManager.getOption(RESTCATALOG_PLUGIN_VIEW_CACHE_ENABLED)).thenReturn(false);
    when(catalog.loadView(TableIdentifier.of("ns1", "v1")))
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "LOAD_VIEW"));

    assertThatThrownBy(() -> accessor.loadView(TableIdentifier.of("ns1", "v1")))
        .satisfies(t -> assertUserException(t, ErrorType.PERMISSION, "load view [ns1.v1]"));
  }

  // --- tables ---

  private Catalog.TableBuilder mockTableBuilder(TableIdentifier id) {
    Catalog.TableBuilder builder = mock(Catalog.TableBuilder.class, RETURNS_SELF);
    when(catalog.buildTable(id, SCHEMA)).thenReturn(builder);
    return builder;
  }

  private void createTable(String... components) {
    accessor.createTable(
        path(components),
        SCHEMA,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        null,
        Collections.emptyMap());
  }

  @Test
  public void testCreateTableSucceeds() {
    TableIdentifier id = TableIdentifier.of("a", "b", "t");
    Table table = mock(Table.class);
    when(mockTableBuilder(id).create()).thenReturn(table);

    assertThat(
            accessor.createTable(
                path("a", "b", "t"),
                SCHEMA,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                null,
                Collections.emptyMap()))
        .isSameAs(table);
  }

  @Test
  public void testCreateTableForbiddenIsPermissionError() {
    TableIdentifier id = TableIdentifier.of("ns1", "t1");
    when(mockTableBuilder(id).create())
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "CREATE_TABLE_DIRECT"));

    assertThatThrownBy(() -> createTable("ns1", "t1"))
        .satisfies(
            t ->
                assertUserException(
                    t, ErrorType.PERMISSION, "create table [ns1.t1]", "CREATE_TABLE_DIRECT"));
  }

  @Test
  public void testCreateTableMissingNamespaceIsValidationError() {
    TableIdentifier id = TableIdentifier.of("nons", "t1");
    when(mockTableBuilder(id).create())
        .thenThrow(new NoSuchNamespaceException("Namespace does not exist: nons"));

    assertThatThrownBy(() -> createTable("nons", "t1"))
        .satisfies(
            t ->
                assertUserException(
                    t, ErrorType.VALIDATION, "folder [nons] does not exist", "Create the folder"));
  }

  @Test
  public void testCreateTableAlreadyExistsIsValidationError() {
    TableIdentifier id = TableIdentifier.of("ns1", "t1");
    when(mockTableBuilder(id).create())
        .thenThrow(new AlreadyExistsException("Table already exists: ns1.t1"));

    assertThatThrownBy(() -> createTable("ns1", "t1"))
        .satisfies(t -> assertUserException(t, ErrorType.VALIDATION, "[ns1.t1] already exists"));
  }

  @Test
  public void testDropTableForbiddenIsPermissionError() {
    when(catalog.dropTable(TableIdentifier.of("ns1", "t1"), false))
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "DROP_TABLE_WITHOUT_PURGE"));

    assertThatThrownBy(() -> accessor.dropTable(path("ns1", "t1")))
        .satisfies(
            t ->
                assertUserException(
                    t, ErrorType.PERMISSION, "drop table [ns1.t1]", "DROP_TABLE_WITHOUT_PURGE"));
  }

  @Test
  public void testDropTableDoesNotPurge() {
    when(catalog.dropTable(TableIdentifier.of("ns1", "t1"), false)).thenReturn(true);

    accessor.dropTable(path("ns1", "t1"));

    verify(catalog).dropTable(TableIdentifier.of("ns1", "t1"), false);
  }

  @Test
  public void testDropTableMissingIsPassedThrough() {
    // RestIcebergCatalogPlugin#dropTable maps it to CatalogEntityNotFoundException.
    when(catalog.dropTable(TableIdentifier.of("ns1", "t1"), false))
        .thenThrow(new NoSuchTableException("Table does not exist: ns1.t1"));

    assertThatThrownBy(() -> accessor.dropTable(path("ns1", "t1")))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  public void testDropTableReturningFalseIsNotFound() {
    // RESTSessionCatalog#dropTable returns false on HTTP 404.
    when(catalog.dropTable(TableIdentifier.of("ns1", "gone"), false)).thenReturn(false);

    assertThatThrownBy(() -> accessor.dropTable(path("ns1", "gone")))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("ns1.gone");
  }

  @Test
  public void testDropTableInMissingNamespaceIsNotFound() {
    when(catalog.dropTable(TableIdentifier.of("nsx", "t1"), false))
        .thenThrow(new NoSuchNamespaceException("Namespace does not exist: nsx"));

    assertThatThrownBy(() -> accessor.dropTable(path("nsx", "t1")))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  public void testDropTableServerErrorIsConnectionError() {
    when(catalog.dropTable(TableIdentifier.of("ns1", "t1"), false))
        .thenThrow(new ServiceFailureException("Server error: %s", "boom"));

    assertThatThrownBy(() -> accessor.dropTable(path("ns1", "t1")))
        .satisfies(
            t ->
                assertUserException(
                    t, ErrorType.CONNECTION, "(HTTP 500) for the request to drop table [ns1.t1]"));
  }

  @Test
  public void testDropTableRejectedRequestIsNotValidationError() {
    // DROP TABLE IF EXISTS reports a validation error as "not found": a drop that the catalog
    // rejects (HTTP 400/422) must not look like a missing table while the table still exists.
    when(catalog.dropTable(TableIdentifier.of("ns1", "t1"), false))
        .thenThrow(new BadRequestException("Malformed request: %s", "cannot drop"));
    when(catalog.dropTable(TableIdentifier.of("ns1", "t2"), false))
        .thenThrow(new UnprocessableEntityException("Unprocessable: %s", "cannot drop"));

    assertThatThrownBy(() -> accessor.dropTable(path("ns1", "t1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.UNSUPPORTED_OPERATION,
                    "rejected the request to drop table [ns1.t1] as invalid (HTTP 400)",
                    "cannot drop"));
    assertThatThrownBy(() -> accessor.dropTable(path("ns1", "t2")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.UNSUPPORTED_OPERATION,
                    "rejected the request to drop table [ns1.t2] as invalid (HTTP 422)"));
  }

  @Test
  public void testRequestFailuresOnWriteOperationsAreMapped() {
    ServiceFailureException failure = new ServiceFailureException("Server error: %s", "boom");
    when(mockTableBuilder(TableIdentifier.of("ns1", "t1")).create()).thenThrow(failure);
    doThrow(failure).when(catalog).createNamespace(eq(Namespace.of("ns2")), anyMap());
    when(catalog.dropNamespace(Namespace.of("ns3"))).thenThrow(failure);
    when(catalog.dropView(TableIdentifier.of("ns1", "v1"))).thenThrow(failure);
    when(catalog.viewExists(TableIdentifier.of("ns1", "v2"))).thenThrow(failure);
    when(catalog.newCreateTableTransaction(TableIdentifier.of("ns1", "c1"), SCHEMA))
        .thenThrow(failure);

    assertThatThrownBy(() -> createTable("ns1", "t1"))
        .satisfies(t -> assertServerError(t, "create table [ns1.t1]"));
    assertThatThrownBy(() -> accessor.createFolder(path("ns2"), Collections.emptyMap()))
        .satisfies(t -> assertServerError(t, "create folder [ns2]"));
    assertThatThrownBy(() -> accessor.dropFolder(path("ns3")))
        .satisfies(t -> assertServerError(t, "drop folder [ns3]"));
    assertThatThrownBy(() -> accessor.dropView(path("ns1", "v1")))
        .satisfies(t -> assertServerError(t, "drop view [ns1.v1]"));
    assertThatThrownBy(
            () ->
                accessor.createView(
                    path("ns1", "v2"), "s3://b/ns1/v2", List.of(SOURCE), SCHEMA, "SELECT 1"))
        .satisfies(t -> assertServerError(t, "create view [ns1.v2]"));
    assertThatThrownBy(
            () ->
                accessor.updateView(
                    path("ns1", "v2"), "s3://b/ns1/v2", List.of(SOURCE), SCHEMA, "SELECT 1"))
        .satisfies(t -> assertServerError(t, "replace view [ns1.v2]"));
    assertThatThrownBy(
            () ->
                accessor.createIcebergTableOperationsForCtas(
                    mock(DremioFileIO.class), path("ns1", "c1"), SCHEMA, null, null))
        .satisfies(t -> assertServerError(t, "create table [ns1.c1]"));
  }

  private static void assertServerError(Throwable t, String actionAndEntity) {
    assertUserException(
        t,
        ErrorType.CONNECTION,
        "(HTTP 500) for the request to " + actionAndEntity,
        "boom",
        RestCatalogExceptionMapper.RETRY_HINT);
  }

  @Test
  public void testCreateTableBadRequestIsValidationError() {
    when(mockTableBuilder(TableIdentifier.of("ns1", "t1")).create())
        .thenThrow(new BadRequestException("Malformed request: %s", "bad location"));

    assertThatThrownBy(() -> createTable("ns1", "t1"))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.VALIDATION,
                    "rejected the request to create table [ns1.t1] as invalid (HTTP 400)",
                    "bad location"));
  }

  @Test
  public void testLoadTableUnclassifiedHttpErrorIsConnectionErrorWithHint() {
    // HTTP 429 (and 502, 504, ...): the Iceberg REST client throws a plain RESTException.
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(false);
    when(catalog.loadTable(TableIdentifier.of("ns1", "t1")))
        .thenThrow(new RESTException("Unable to process: %s", "rate limited"));

    assertThatThrownBy(() -> accessor.loadTable(TableIdentifier.of("ns1", "t1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.CONNECTION,
                    "unexpected HTTP error for the request to load table [ns1.t1]",
                    "Unable to process: rate limited.",
                    RestCatalogExceptionMapper.UNCLASSIFIED_STATUS_NOTE))
        .hasCauseInstanceOf(RESTException.class);
  }

  @Test
  public void testLoadTableThroughCacheTimeoutIsConnectionErrorWithHint() {
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(true);
    when(catalog.loadTable(TableIdentifier.of("ns1", "t1")))
        .thenThrow(
            new RESTException(
                new SocketTimeoutException("Read timed out"),
                "Error occurred while processing %s request",
                "GET"));

    assertThatThrownBy(() -> accessor.loadTable(TableIdentifier.of("ns1", "t1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.CONNECTION,
                    "did not respond in time to the request to load table [ns1.t1]",
                    "(Read timed out)",
                    RestCatalogExceptionMapper.TIMEOUT_HINT));
  }

  @Test
  public void testLoadTableServiceUnavailableMentionsRetries() {
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(false);
    when(catalog.loadTable(TableIdentifier.of("ns1", "t1")))
        .thenThrow(new ServiceUnavailableException("Service unavailable: %s", "down"));

    assertThatThrownBy(() -> accessor.loadTable(TableIdentifier.of("ns1", "t1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.CONNECTION,
                    "unavailable (HTTP 503)",
                    RestCatalogExceptionMapper.RETRIED_NOTE));
  }

  @Test
  public void testLoadViewServerErrorIsConnectionError() {
    when(optionManager.getOption(RESTCATALOG_PLUGIN_VIEW_CACHE_ENABLED)).thenReturn(false);
    when(catalog.loadView(TableIdentifier.of("ns1", "v1")))
        .thenThrow(new ServiceFailureException("Server error: %s", "boom"));

    assertThatThrownBy(() -> accessor.loadView(TableIdentifier.of("ns1", "v1")))
        .satisfies(t -> assertUserException(t, ErrorType.CONNECTION, "load view [ns1.v1]"));
  }

  @Test
  public void testRequestFailedRedactsServerTextAndCause() {
    RESTException e = new RESTException("Unable to process: %s", "echo s3cr3t-value");
    UserException mapped =
        RestCatalogExceptionMapper.requestFailed(
            e, "look up", "[ns1.t1]", text -> text.replace("s3cr3t-value", "****"));

    assertThat(mapped.getOriginalMessage()).contains("echo ****").doesNotContain("s3cr3t");
    assertThat(mapped.getCause()).isInstanceOf(RESTException.class);
    assertThat(mapped.getCause().getMessage()).doesNotContain("s3cr3t");
    assertThat(mapped.getCause().getStackTrace()).isEqualTo(e.getStackTrace());
  }

  @Test
  public void testRequestFailedClassifiesNetworkFailures() {
    assertThat(
            RestCatalogExceptionMapper.requestFailed(
                    new RESTException(new ConnectException("Connection refused"), "Error occurred"),
                    "look up",
                    "[t]")
                .getOriginalMessage())
        .contains("Unable to reach")
        .contains(RestCatalogExceptionMapper.UNREACHABLE_HINT);
    assertThat(
            RestCatalogExceptionMapper.requestFailed(
                    new RESTException(new SSLHandshakeException("bad cert"), "Error occurred"),
                    "look up",
                    "[t]")
                .getOriginalMessage())
        .contains("TLS connection")
        .contains(RestCatalogExceptionMapper.TLS_HINT);
    assertThat(
            RestCatalogExceptionMapper.requestFailed(
                    new RESTException(new IOException("Connection reset"), "Error occurred"),
                    "look up",
                    "[t]")
                .getOriginalMessage())
        .contains("connection to the Iceberg REST catalog failed")
        .contains("(Connection reset)")
        .contains(RestCatalogExceptionMapper.CONNECTION_RETRIED_NOTE);
  }

  @Test
  public void testNotAuthorizedOnWriteOperationsIsPermissionError() {
    NotAuthorizedException expired = new NotAuthorizedException("Not authorized: %s", "expired");
    when(mockTableBuilder(TableIdentifier.of("ns1", "t1")).create()).thenThrow(expired);
    doThrow(expired).when(catalog).createNamespace(eq(Namespace.of("ns2")), anyMap());
    when(catalog.dropNamespace(Namespace.of("ns3"))).thenThrow(expired);
    when(catalog.dropView(TableIdentifier.of("ns1", "v1"))).thenThrow(expired);
    // The existence check before creating or replacing a view is mapped too.
    when(catalog.viewExists(TableIdentifier.of("ns1", "v2"))).thenThrow(expired);
    when(catalog.newCreateTableTransaction(TableIdentifier.of("ns1", "c1"), SCHEMA))
        .thenThrow(expired);

    assertThatThrownBy(() -> createTable("ns1", "t1"))
        .satisfies(t -> assertNotAuthorized(t, "create table [ns1.t1]"));
    assertThatThrownBy(() -> accessor.createFolder(path("ns2"), Collections.emptyMap()))
        .satisfies(t -> assertNotAuthorized(t, "create folder [ns2]"));
    assertThatThrownBy(() -> accessor.dropFolder(path("ns3")))
        .satisfies(t -> assertNotAuthorized(t, "drop folder [ns3]"));
    assertThatThrownBy(() -> accessor.dropView(path("ns1", "v1")))
        .satisfies(t -> assertNotAuthorized(t, "drop view [ns1.v1]"));
    assertThatThrownBy(
            () ->
                accessor.createView(
                    path("ns1", "v2"), "s3://b/ns1/v2", List.of(SOURCE), SCHEMA, "SELECT 1"))
        .satisfies(t -> assertNotAuthorized(t, "create view [ns1.v2]"));
    assertThatThrownBy(
            () ->
                accessor.updateView(
                    path("ns1", "v2"), "s3://b/ns1/v2", List.of(SOURCE), SCHEMA, "SELECT 1"))
        .satisfies(t -> assertNotAuthorized(t, "replace view [ns1.v2]"));
    assertThatThrownBy(
            () ->
                accessor.createIcebergTableOperationsForCtas(
                    mock(DremioFileIO.class), path("ns1", "c1"), SCHEMA, null, null))
        .satisfies(t -> assertNotAuthorized(t, "create table [ns1.c1]"));
  }

  private static void assertNotAuthorized(Throwable t, String actionAndEntity) {
    assertUserException(
        t,
        ErrorType.PERMISSION,
        "rejected the credentials of the request to " + actionAndEntity,
        RestCatalogExceptionMapper.NOT_AUTHORIZED_HINT);
  }

  @Test
  public void testLoadTableBadRequestIsValidationError() {
    // BadRequestException (HTTP 400) is a RESTException too, but not a connection problem.
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(false);
    when(catalog.loadTable(TableIdentifier.of("ns1", "t1")))
        .thenThrow(new BadRequestException("Malformed request: %s", "bad identifier"));

    assertThatThrownBy(() -> accessor.loadTable(TableIdentifier.of("ns1", "t1")))
        .satisfies(
            t ->
                assertUserException(
                    t,
                    ErrorType.VALIDATION,
                    "rejected the request to load table [ns1.t1] as invalid (HTTP 400)",
                    "Malformed request: bad identifier."));
    assertThat(
            RestCatalogExceptionMapper.requestFailed(
                    new UnprocessableEntityException("Unable to process: %s", "bad"),
                    "look up",
                    "[t]")
                .getErrorType())
        .isEqualTo(ErrorType.VALIDATION);
  }

  @Test
  public void testRequestFailedWithMarkupOnlyMessageNamesTheExceptionType() {
    UserException mapped =
        RestCatalogExceptionMapper.requestFailed(
            new RESTException("%s", "<html><body><hr></body></html>"), "look up", "[t]");

    assertThat(mapped.getOriginalMessage())
        .contains("unexpected HTTP error for the request to look up [t]: RESTException.");
  }

  @Test
  public void testRedactedForLoggingDropsSecretsFromTheCauseChain() {
    UnaryOperator<String> redactor = text -> text.replace("s3cr3t-value", "****");
    RESTException clean = new RESTException("Unable to process: %s", "boom");
    assertThat(RestCatalogExceptionMapper.redactedForLogging(clean, redactor)).isSameAs(clean);

    RESTException leaky =
        new RESTException(
            new IOException("echo s3cr3t-value"), "Unable to process: %s", "echo s3cr3t-value");
    Throwable logged = RestCatalogExceptionMapper.redactedForLogging(leaky, redactor);
    assertThat(logged.getMessage())
        .isEqualTo(RESTException.class.getName() + ": Unable to process: echo ****");
    assertThat(logged.getCause()).isNull();
    assertThat(logged.getStackTrace()).isEqualTo(leaky.getStackTrace());

    RESTException leakyCause =
        new RESTException(new IOException("echo s3cr3t-value"), "Unable to process");
    assertThat(RestCatalogExceptionMapper.redactedForLogging(leakyCause, redactor).getCause())
        .isNull();
  }

  @Test
  public void testLoadTableForbiddenIsPermissionError() {
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(false);
    when(catalog.loadTable(TableIdentifier.of("ns1", "t1")))
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "LOAD_TABLE_WITH_READ_DELEGATION"));

    assertThatThrownBy(() -> accessor.loadTable(TableIdentifier.of("ns1", "t1")))
        .satisfies(t -> assertUserException(t, ErrorType.PERMISSION, "load table [ns1.t1]"));
  }

  @Test
  public void testLoadTableForbiddenThroughCacheIsPermissionError() {
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(true);
    when(catalog.loadTable(TableIdentifier.of("ns1", "t1")))
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "LOAD_TABLE_WITH_READ_DELEGATION"));

    assertThatThrownBy(() -> accessor.loadTable(TableIdentifier.of("ns1", "t1")))
        .satisfies(t -> assertUserException(t, ErrorType.PERMISSION, "load table [ns1.t1]"));
  }

  @Test
  public void testLoadTableMissingIsPassedThrough() {
    when(optionManager.getOption(RESTCATALOG_PLUGIN_TABLE_CACHE_ENABLED)).thenReturn(false);
    when(catalog.loadTable(TableIdentifier.of("ns1", "t1")))
        .thenThrow(new NoSuchTableException("Table does not exist: ns1.t1"));

    assertThatThrownBy(() -> accessor.loadTable(TableIdentifier.of("ns1", "t1")))
        .isInstanceOf(NoSuchTableException.class);
  }

  // --- commits ---

  @SuppressWarnings("unchecked")
  private static TableOperations mockRestTableOperations() throws ClassNotFoundException {
    // RESTTableOperations is package-private; DremioRESTTableOperations casts to it.
    Class<? extends TableOperations> restOps =
        (Class<? extends TableOperations>)
            Class.forName("org.apache.iceberg.rest.RESTTableOperations");
    return mock(restOps);
  }

  @Test
  public void testCommitForbiddenIsPermissionError() throws Exception {
    TableOperations delegate = mockRestTableOperations();
    TableMetadata base = mock(TableMetadata.class);
    TableMetadata updated = mock(TableMetadata.class);
    doThrow(new ForbiddenException(POLARIS_GRANT_403, "UPDATE_TABLE"))
        .when(delegate)
        .commit(base, updated);
    TableOperations ops =
        new AbstractRestCatalogAccessor.ForbiddenMappingTableOperations(
            mock(DremioFileIO.class), delegate, path("ns1", "t1"), UnaryOperator.identity());

    assertThatThrownBy(() -> ops.commit(base, updated))
        // Still a CleanableFailure: Iceberg deletes the manifests written for the rejected commit.
        .isInstanceOf(AbstractRestCatalogAccessor.CommitForbiddenException.class)
        .isInstanceOf(CleanableFailure.class)
        .hasMessageContaining("commit to table [polaris.ns1.t1]")
        .satisfies(
            t -> {
              // Whatever Dremio wraps it in reports the permission error.
              assertUserException(
                  UserException.systemError(t).buildSilently(),
                  ErrorType.PERMISSION,
                  "commit to table [polaris.ns1.t1]",
                  "UPDATE_TABLE");
              assertUserException(
                  UserException.dataWriteError(new RuntimeException(t)).buildSilently(),
                  ErrorType.PERMISSION,
                  "UPDATE_TABLE");
            });
  }

  @Test
  public void testCommitNotAuthorizedIsPermissionError() throws Exception {
    TableOperations delegate = mockRestTableOperations();
    TableMetadata base = mock(TableMetadata.class);
    TableMetadata updated = mock(TableMetadata.class);
    doThrow(new NotAuthorizedException("Not authorized: %s", "expired"))
        .when(delegate)
        .commit(base, updated);
    TableOperations ops =
        new AbstractRestCatalogAccessor.ForbiddenMappingTableOperations(
            mock(DremioFileIO.class), delegate, path("ns1", "t1"), UnaryOperator.identity());

    assertThatThrownBy(() -> ops.commit(base, updated))
        .isInstanceOf(AbstractRestCatalogAccessor.CommitNotAuthorizedException.class)
        .isInstanceOf(CleanableFailure.class)
        .satisfies(
            t ->
                assertNotAuthorized(
                    UserException.systemError(t).buildSilently(),
                    "commit to table [polaris.ns1.t1]"));
  }

  @Test
  public void testServerMessageIsRedactedInMessageAndCause() {
    String secret = "secret-value-0042";
    UserException e =
        RestCatalogExceptionMapper.forbidden(
            new ForbiddenException("Forbidden: echo %s", secret),
            "drop view",
            "[ns1.v1]",
            text -> text.replace(secret, "****"));

    assertThat(e.getOriginalMessage()).contains("echo ****").doesNotContain(secret);
    for (Throwable t = e; t != null; t = t.getCause()) {
      assertThat(String.valueOf(t.getMessage())).doesNotContain(secret);
    }
    assertThat(e.getCause()).isInstanceOf(ForbiddenException.class);

    UserException unauthorized =
        RestCatalogExceptionMapper.notAuthorized(
            new NotAuthorizedException("Not authorized: echo %s", secret),
            "look up",
            "[ns1.t1]",
            text -> text.replace(secret, "****"));
    assertThat(unauthorized.getOriginalMessage()).contains("echo ****").doesNotContain(secret);
    assertThat(unauthorized.getCause())
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessageNotContaining(secret);
  }

  @Test
  public void testCommitConflictIsPassedThrough() throws Exception {
    // IcebergBaseCommand and the committers turn CommitFailedException (HTTP 409) into a
    // concurrent modification error, so it must reach them unchanged.
    TableOperations delegate = mockRestTableOperations();
    TableMetadata base = mock(TableMetadata.class);
    TableMetadata updated = mock(TableMetadata.class);
    CommitFailedException conflict =
        new CommitFailedException("Commit failed: Requirement failed: branch main has changed");
    doThrow(conflict).when(delegate).commit(base, updated);
    TableOperations ops =
        new AbstractRestCatalogAccessor.ForbiddenMappingTableOperations(
            mock(DremioFileIO.class), delegate, path("ns1", "t1"), UnaryOperator.identity());

    assertThatThrownBy(() -> ops.commit(base, updated)).isSameAs(conflict);
  }

  @Test
  public void testCommitSuccessDelegates() throws Exception {
    TableOperations delegate = mockRestTableOperations();
    TableMetadata base = mock(TableMetadata.class);
    TableMetadata updated = mock(TableMetadata.class);
    TableOperations ops =
        new AbstractRestCatalogAccessor.ForbiddenMappingTableOperations(
            mock(DremioFileIO.class), delegate, path("ns1", "t1"), UnaryOperator.identity());

    ops.commit(base, updated);

    verify(delegate).commit(base, updated);
  }

  // --- mapper ---

  @Test
  public void testNamespaceNotEmptyDetection() {
    assertThat(
            RestCatalogExceptionMapper.isNamespaceNotEmpty(
                new BadRequestException("Malformed request: Namespace ns1 is not empty")))
        .isTrue();
    assertThat(
            RestCatalogExceptionMapper.isNamespaceNotEmpty(
                new BadRequestException("Malformed request: Namespace NS1 is NOT EMPTY")))
        .isTrue();
    assertThat(
            RestCatalogExceptionMapper.isNamespaceNotEmpty(
                new BadRequestException("Malformed request: invalid name")))
        .isFalse();
  }

  @Test
  public void testForbiddenWithoutMessage() {
    UserException e =
        RestCatalogExceptionMapper.forbidden(
            new ForbiddenException("%s", ""), "drop view", "[ns1.v1]");

    assertThat(e.getErrorType()).isEqualTo(ErrorType.PERMISSION);
    assertThat(e.getOriginalMessage())
        .contains("drop view [ns1.v1]: ForbiddenException.")
        .contains(RestCatalogExceptionMapper.MISSING_PRIVILEGE_HINT);
  }

  @Test
  public void testForbiddenMessageWithPercentIsNotFormatted() {
    UserException e =
        RestCatalogExceptionMapper.forbidden(
            new ForbiddenException("%s", "Forbidden: 100% denied"), "drop table", "[a.b]");

    assertThat(e.getOriginalMessage()).contains("Forbidden: 100% denied.");
  }

  @Test
  public void testPurgeDetectionIgnoresPrivilegeNames() {
    assertThat(RestCatalogExceptionMapper.isPurgeRelated(POLARIS_PURGE_403)).isTrue();
    assertThat(
            RestCatalogExceptionMapper.isPurgeRelated(
                String.format(POLARIS_GRANT_403, "DROP_TABLE_WITHOUT_PURGE")))
        .isFalse();
    assertThat(
            RestCatalogExceptionMapper.isPurgeRelated(
                String.format(POLARIS_GRANT_403, "DROP_TABLE_WITH_PURGE")))
        .isFalse();
  }

  @Test
  public void testDropTableWithoutPurgePrivilegeHasPrivilegeHint() {
    when(catalog.dropTable(TableIdentifier.of("ns1", "t1"), false))
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "DROP_TABLE_WITHOUT_PURGE"));

    assertThatThrownBy(() -> accessor.dropTable(path("ns1", "t1")))
        .satisfies(
            t ->
                assertThat(((UserException) t).getOriginalMessage())
                    .contains(RestCatalogExceptionMapper.MISSING_PRIVILEGE_HINT)
                    .doesNotContain(RestCatalogExceptionMapper.DROP_WITH_PURGE_HINT));
  }

  @Test
  public void testCtasStagingForbiddenIsPermissionError() {
    when(catalog.newCreateTableTransaction(TableIdentifier.of("ns1", "c1"), SCHEMA))
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "CREATE_TABLE_STAGED"));

    assertThatThrownBy(
            () ->
                accessor.createIcebergTableOperationsForCtas(
                    mock(DremioFileIO.class), path("ns1", "c1"), SCHEMA, null, null))
        .satisfies(
            t ->
                assertUserException(
                    t, ErrorType.PERMISSION, "create table [ns1.c1]", "CREATE_TABLE_STAGED"));
  }

  // --- CTAS (staged create) ---

  private BaseTransaction stagedCreate(TableOperations ops) {
    BaseTransaction transaction = mock(BaseTransaction.class);
    when(transaction.underlyingOps()).thenReturn(ops);
    return transaction;
  }

  @Test
  public void testPartitionedCtasCommitsThroughCreationStagedWithItsSpec() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "region", Types.StringType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.builderFor(schema).identity("region").build(),
            SortOrder.unsorted(),
            "s3://bucket/ns1/c1",
            Collections.emptyMap());
    // The cause: fresh table metadata adds the partition spec without setting it as the default
    // (a new table starts with default spec id 0), so committing it on top of the creation staged
    // with only the schema left the unpartitioned spec as the default.
    assertThat(metadata.changes())
        .noneMatch(change -> change instanceof MetadataUpdate.SetDefaultPartitionSpec);

    TableIdentifier identifier = TableIdentifier.of("ns1", "c1");
    TableOperations schemaOnlyStage = mockRestTableOperations();
    BaseTransaction schemaOnlyTransaction = stagedCreate(schemaOnlyStage);
    when(catalog.newCreateTableTransaction(identifier, SCHEMA)).thenReturn(schemaOnlyTransaction);
    TableOperations specStage = mockRestTableOperations();
    TableMetadata committed = mock(TableMetadata.class);
    when(specStage.current()).thenReturn(committed);
    Catalog.TableBuilder builder = mock(Catalog.TableBuilder.class, RETURNS_SELF);
    BaseTransaction specTransaction = stagedCreate(specStage);
    when(builder.createTransaction()).thenReturn(specTransaction);
    when(catalog.buildTable(identifier, metadata.schema())).thenReturn(builder);

    TableOperations ops =
        accessor.createIcebergTableOperationsForCtas(
            mock(DremioFileIO.class), path("ns1", "c1"), SCHEMA, null, null);
    ops.commit(null, metadata);

    verify(builder).withPartitionSpec(metadata.spec());
    verify(builder).withSortOrder(metadata.sortOrder());
    verify(builder).withLocation("s3://bucket/ns1/c1");
    verify(builder).withProperties(metadata.properties());
    verify(specStage).commit(null, metadata);
    verify(schemaOnlyStage, never()).commit(any(), any());
    assertThat(ops.current()).isSameAs(committed);
  }

  @Test
  public void testUnpartitionedCtasCommitsThroughSchemaOnlyStage() throws Exception {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), "s3://bucket/ns1/c1", Collections.emptyMap());
    TableIdentifier identifier = TableIdentifier.of("ns1", "c1");
    TableOperations schemaOnlyStage = mockRestTableOperations();
    BaseTransaction schemaOnlyTransaction = stagedCreate(schemaOnlyStage);
    when(catalog.newCreateTableTransaction(identifier, SCHEMA)).thenReturn(schemaOnlyTransaction);

    TableOperations ops =
        accessor.createIcebergTableOperationsForCtas(
            mock(DremioFileIO.class), path("ns1", "c1"), SCHEMA, null, null);
    ops.commit(null, metadata);

    verify(schemaOnlyStage).commit(null, metadata);
    verify(catalog, never()).buildTable(any(), any());
  }

  @Test
  public void testPartitionedCtasRestageForbiddenIsPermissionError() throws Exception {
    Schema schema = new Schema(Types.NestedField.optional(1, "region", Types.StringType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.builderFor(schema).identity("region").build(),
            "s3://bucket/ns1/c1",
            Collections.emptyMap());
    TableIdentifier identifier = TableIdentifier.of("ns1", "c1");
    BaseTransaction schemaOnlyTransaction = stagedCreate(mockRestTableOperations());
    when(catalog.newCreateTableTransaction(identifier, SCHEMA)).thenReturn(schemaOnlyTransaction);
    Catalog.TableBuilder builder = mock(Catalog.TableBuilder.class, RETURNS_SELF);
    when(builder.createTransaction())
        .thenThrow(new ForbiddenException(POLARIS_GRANT_403, "CREATE_TABLE_STAGED"));
    when(catalog.buildTable(identifier, metadata.schema())).thenReturn(builder);

    TableOperations ops =
        accessor.createIcebergTableOperationsForCtas(
            mock(DremioFileIO.class), path("ns1", "c1"), SCHEMA, null, null);

    assertThatThrownBy(() -> ops.commit(null, metadata))
        .isInstanceOf(AbstractRestCatalogAccessor.CommitForbiddenException.class)
        .hasMessageContaining("CREATE_TABLE_STAGED");
  }
}
