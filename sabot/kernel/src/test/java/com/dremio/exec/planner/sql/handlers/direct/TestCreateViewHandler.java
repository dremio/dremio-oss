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
package com.dremio.exec.planner.sql.handlers.direct;

import static com.dremio.exec.ExecConstants.VERSIONED_VIEW_ENABLED;
import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.VALIDATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProviderFactory;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.planner.sql.parser.SqlCreateView;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.proto.UserBitShared.UserCredentials;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.DremioTest;
import com.dremio.test.UserExceptionAssert;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.CancelFlag;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

/** This test will only include unit tests for versioned views */
public class TestCreateViewHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "dataplane_source_1";
  private static final String VIEW_PATH = "view1";
  private static final String DEFAULT_BRANCH_NAME = "branchName";
  private static final NamespaceKey DEFAULT_SOURCE_KEY = new NamespaceKey(DEFAULT_SOURCE_NAME);

  private static final NamespaceKey VIEW_PATH_NS = new NamespaceKey(VIEW_PATH);
  private static final NamespaceKey RESOLVED_NAMESPACE_VIEW_KEY =
      new NamespaceKey(Arrays.asList(DEFAULT_SOURCE_NAME, VIEW_PATH));

  private static final CatalogEntityKey RESOLVED_CATALOG_ENTITY_VIEW_KEY =
      CatalogEntityKey.newBuilder()
          .keyComponents(Arrays.asList(DEFAULT_SOURCE_NAME, VIEW_PATH))
          .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
          .build();
  private static final NamespaceKey DEFAULT_SCHEMA = DEFAULT_SOURCE_KEY;
  private static final VersionContext DEFAULT_VERSION =
      VersionContext.ofBranch(DEFAULT_BRANCH_NAME);
  private static final ResolvedVersionContext DEFAULT_RESOLVED_VERSION_CONTEXT =
      ResolvedVersionContext.ofBranch(DEFAULT_BRANCH_NAME, "0123456789abcdeff");
  protected static final VersionContext sessionVersion = VersionContext.NOT_SPECIFIED;
  private static final String DEFAULT_SQL = "select * from dataplane_source_1.t1";
  private static final SqlNodeList SQL_NODE_LIST =
      new SqlNodeList(Collections.emptyList(), SqlParserPos.ZERO);
  private static SqlNode sqlNode = mock(SqlNode.class);
  private static SqlCreateView default_input =
      new SqlCreateView(
          SqlParserPos.ZERO,
          new SqlIdentifier(VIEW_PATH, SqlParserPos.ZERO),
          SQL_NODE_LIST,
          sqlNode,
          false,
          null,
          null,
          null);

  private static SqlCreateView default_replace_input =
      new SqlCreateView(
          SqlParserPos.ZERO,
          new SqlIdentifier(VIEW_PATH, SqlParserPos.ZERO),
          SQL_NODE_LIST,
          sqlNode,
          true,
          null,
          null,
          null);

  private static SqlCreateView invalid_input =
      new SqlCreateView(
          SqlParserPos.ZERO,
          new SqlIdentifier(VIEW_PATH, SqlParserPos.ZERO),
          SQL_NODE_LIST,
          sqlNode,
          true,
          null,
          null,
          null);

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);
  @Mock private Catalog catalog;
  @Mock private UserSession userSession;
  @Mock private SqlHandlerConfig config;
  @Mock private QueryContext context;
  @Mock private SqlString queryString;
  @Mock private OptionManager optionManager;
  @Mock private View view;

  // Start of mocks for getViewSql tests
  @Mock private AttemptObserver observer;
  @Mock private PlannerSettings settings;
  @Mock private MaterializationDescriptorProvider materializationProvider;
  @Mock private FunctionImplementationRegistry functions;
  @Mock private SubstitutionProviderFactory factory;
  @Mock private SubstitutionProvider substitutionProvider;
  @Mock private SabotConfig sabotConfig;
  @Mock private OptionResolver options;
  @Mock private UserCredentials userCredentials;
  @Mock private ContextInformation contextInformation;
  @Mock private SqlHandlerConfig sqlHandlerConfig;
  @Mock private ConvertedRelNode convertedRelNode;
  @Mock private ResolvedVersionContext resolvedVersionContext;
  @Mock private StoragePlugin storagePlugin;

  // End of mocks for getViewSql tests

  private BatchSchema batchSchema = new BatchSchema(new ArrayList<>());
  private ViewOptions createViewOptions =
      new ViewOptions.ViewOptionsBuilder()
          .version(DEFAULT_RESOLVED_VERSION_CONTEXT)
          .batchSchema(batchSchema)
          .build();
  private ViewOptions replaceViewOptions =
      new ViewOptions.ViewOptionsBuilder()
          .version(DEFAULT_RESOLVED_VERSION_CONTEXT)
          .batchSchema(batchSchema)
          .actionType(ViewOptions.ActionType.UPDATE_VIEW)
          .build();
  private CreateViewHandler createViewHandler;
  private SqlConverter parser;

  @Test
  public void createVersionedViewSuccessful() throws Exception {
    setupResources();
    when(config.getContext().getSession().getSessionVersionForSource(DEFAULT_SOURCE_NAME))
        .thenReturn(sessionVersion);
    doReturn(resolvedVersionContext)
        .when(createViewHandler)
        .getResolvedVersionContext(DEFAULT_SOURCE_NAME, sessionVersion);
    doReturn(replaceViewOptions)
        .when(createViewHandler)
        .getViewOptions(false, resolvedVersionContext);
    doReturn(true)
        .when(createViewHandler)
        .checkViewExistence(
            VIEW_PATH, false, CatalogEntityKey.fromNamespaceKey(RESOLVED_NAMESPACE_VIEW_KEY));
    doReturn(view).when(createViewHandler).getView(default_input, "", convertedRelNode);
    doNothing().when(catalog).createView(RESOLVED_NAMESPACE_VIEW_KEY, view, createViewOptions);
    doReturn("").when(createViewHandler).getViewSql(default_input, "");
    doReturn(RESOLVED_NAMESPACE_VIEW_KEY).when(catalog).resolveSingle(VIEW_PATH_NS);
    doReturn(convertedRelNode)
        .when(createViewHandler)
        .validateTablesAndVersionContext(sqlNode, RESOLVED_NAMESPACE_VIEW_KEY, sessionVersion);
    List<SimpleCommandResult> result = createViewHandler.toResult("", default_input);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("created successfully").contains("View");
  }

  @Test
  public void testCreateViewTableNotFound() throws Exception {
    setupResources();
    when(config.getContext().getSession().getSessionVersionForSource(DEFAULT_SOURCE_NAME))
        .thenReturn(sessionVersion);
    doReturn("").when(createViewHandler).getViewSql(default_input, "");
    doReturn(RESOLVED_NAMESPACE_VIEW_KEY).when(catalog).resolveSingle(VIEW_PATH_NS);
    MockedStatic<SqlToRelTransformer> prelTransformerMockedStatic =
        mockStatic(SqlToRelTransformer.class);
    prelTransformerMockedStatic
        .when(
            () ->
                SqlToRelTransformer.validateAndConvert(
                    any(SqlHandlerConfig.class), eq(default_input.getQuery())))
        .thenThrow(UserException.validationError().message("not found").buildSilently());
    // Cannot find relNode (Table) if we don't set up one in test
    assertThatThrownBy(() -> createViewHandler.toResult("", default_input))
        .hasMessageContaining("Validation of view sql failed");
  }

  @Test
  public void testCreateViewTableException() throws Exception {
    setupResources();
    MockedStatic<SqlToRelTransformer> prelTransformerMockedStatic =
        mockStatic(SqlToRelTransformer.class);
    doReturn("").when(createViewHandler).getViewSql(default_input, "");
    doReturn(RESOLVED_NAMESPACE_VIEW_KEY).when(catalog).resolveSingle(VIEW_PATH_NS);
    when(config.getContext().getSession().getSessionVersionForSource(DEFAULT_SOURCE_NAME))
        .thenReturn(sessionVersion);
    prelTransformerMockedStatic
        .when(
            () ->
                SqlToRelTransformer.validateAndConvert(
                    any(SqlHandlerConfig.class), eq(default_input.getQuery())))
        .thenThrow(
            UserException.validationError()
                .message("Unable to fix vds %s", VIEW_PATH)
                .buildSilently());
    // Repair of underlying  view in relNode  fails with "Unable to fix" exception
    String expectedDetailedMessage = String.format("Unable to fix vds " + VIEW_PATH);
    assertThatThrownBy(() -> createViewHandler.toResult("", default_input))
        .hasMessageContaining(String.format("Validation of view sql failed"));
  }

  @Test
  public void testCreateViewInvalidQueryDrop() throws Exception {
    testCreateViewInvalidQuery(SqlKind.DROP_TABLE);
  }

  @Test
  public void testCreateViewInvalidQueryCreate() throws Exception {
    testCreateViewInvalidQuery(SqlKind.CREATE_TABLE);
  }

  @Test
  public void testCreateViewInvalidQueryUpdate() throws Exception {
    testCreateViewInvalidQuery(SqlKind.UPDATE);
  }

  @Test
  public void testCreateViewInvalidQueryInsert() throws Exception {
    testCreateViewInvalidQuery(SqlKind.INSERT);
  }

  @Test
  public void testCreateViewInvalidQueryMerge() throws Exception {
    testCreateViewInvalidQuery(SqlKind.MERGE);
  }

  @Test
  public void testCreateViewInvalidQueryDelete() throws Exception {
    testCreateViewInvalidQuery(SqlKind.DELETE);
  }

  private void testCreateViewInvalidQuery(SqlKind kind) throws Exception {
    when(context.getCatalog()).thenReturn(catalog);
    when(config.getContext()).thenReturn(context);
    when(optionManager.getOption(VERSIONED_VIEW_ENABLED)).thenReturn(true);
    when(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT)).thenReturn(queryString);
    when(sqlNode.isA(anySet())).thenCallRealMethod();
    when(sqlNode.getKind()).thenReturn(kind);
    when(context.getOptions()).thenReturn(optionManager);
    when(config.getContext()).thenReturn(context);
    createViewHandler = spy(new CreateViewHandler(config));
    assertThatThrownBy(() -> createViewHandler.toResult("", invalid_input))
        .hasMessageContaining("Cannot create view on statement of this type");
  }

  @Test
  public void replaceVersionedViewSuccessful() throws Exception {
    setupResources();
    when(config.getContext().getSession().getSessionVersionForSource(DEFAULT_SOURCE_NAME))
        .thenReturn(sessionVersion);
    doReturn(resolvedVersionContext)
        .when(createViewHandler)
        .getResolvedVersionContext(DEFAULT_SOURCE_NAME, sessionVersion);
    doReturn(replaceViewOptions)
        .when(createViewHandler)
        .getViewOptions(true, resolvedVersionContext);
    doReturn(true)
        .when(createViewHandler)
        .checkViewExistence(
            VIEW_PATH, true, CatalogEntityKey.fromNamespaceKey(RESOLVED_NAMESPACE_VIEW_KEY));
    doReturn(RESOLVED_NAMESPACE_VIEW_KEY).when(catalog).resolveSingle(VIEW_PATH_NS);
    doReturn(view).when(createViewHandler).getView(default_replace_input, "", null);
    doNothing().when(catalog).updateView(RESOLVED_NAMESPACE_VIEW_KEY, view, replaceViewOptions);
    doReturn("").when(createViewHandler).getViewSql(default_replace_input, "");
    doReturn(null)
        .when(createViewHandler)
        .validateTablesAndVersionContext(sqlNode, RESOLVED_NAMESPACE_VIEW_KEY, sessionVersion);

    List<SimpleCommandResult> result = createViewHandler.toResult("", default_replace_input);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("replaced successfully").contains("View");
  }

  @Test
  public void replaceVersionedViewNameClash() throws Exception {
    setupResources();
    DremioTable dremioTable = mock(DremioTable.class);
    Schema.TableType tableType = Schema.TableType.TABLE;
    when(config.getContext().getSession().getSessionVersionForSource(DEFAULT_SOURCE_NAME))
        .thenReturn(sessionVersion);
    doReturn(resolvedVersionContext)
        .when(createViewHandler)
        .getResolvedVersionContext(DEFAULT_SOURCE_NAME, sessionVersion);
    doReturn(replaceViewOptions)
        .when(createViewHandler)
        .getViewOptions(true, resolvedVersionContext);
    doReturn(view).when(createViewHandler).getView(default_replace_input, "", null);
    doReturn(true).when(createViewHandler).isVersioned(RESOLVED_NAMESPACE_VIEW_KEY);
    when(resolvedVersionContext.getType()).thenReturn(ResolvedVersionContext.Type.BRANCH);
    when(resolvedVersionContext.getRefName()).thenReturn("main");
    doReturn(dremioTable).when(catalog).getTableNoResolve(RESOLVED_CATALOG_ENTITY_VIEW_KEY);
    doReturn(RESOLVED_NAMESPACE_VIEW_KEY).when(catalog).resolveSingle(VIEW_PATH_NS);
    doReturn(tableType).when(dremioTable).getJdbcTableType();
    doReturn("").when(createViewHandler).getViewSql(default_replace_input, "");
    doReturn(null)
        .when(createViewHandler)
        .validateTablesAndVersionContext(sqlNode, RESOLVED_NAMESPACE_VIEW_KEY, sessionVersion);
    UserExceptionAssert.assertThatThrownBy(
            () -> createViewHandler.toResult("", default_replace_input))
        .hasErrorType(VALIDATION)
        .hasMessageContaining("A non-view table with given name")
        .hasMessageContaining("already exists in schema");
  }

  private void setupResources() throws SqlParseException {
    setupCreateViewHandler();
    // versioned view test only
    doReturn(true).when(createViewHandler).isVersioned(RESOLVED_NAMESPACE_VIEW_KEY);
  }

  private void setupCreateViewHandler() {
    when(catalog.resolveSingle(default_input.getPath())).thenReturn(DEFAULT_SOURCE_KEY);
    when(context.getCatalog()).thenReturn(catalog);
    when(optionManager.getOption(VERSIONED_VIEW_ENABLED)).thenReturn(true);
    when(context.getOptions()).thenReturn(optionManager);
    when(config.getContext()).thenReturn(context);
    when(context.getSession()).thenReturn(userSession);
    when(userSession.getDefaultSchemaPath()).thenReturn(DEFAULT_SCHEMA);
    when(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT)).thenReturn(queryString);
    when(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT, true)).thenReturn(queryString);
    when(sqlNode.getKind()).thenReturn(SqlKind.SELECT);
    createViewHandler = spy(new CreateViewHandler(config));
  }

  @Test
  public void testGetViewSqlDoubleQuoted() throws UserException {
    setupGetViewSqlResources(Quoting.DOUBLE_QUOTE);
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE VIEW foo\n AS\nSELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE VIEW\n foo AS SELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE \nVIEW foo AS\n\t SELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE \nVIEW foo AS\n\t SELECT\n* FROM \nbar", "SELECT\n* FROM \nbar");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM \"bar\"", "SELECT * FROM \"bar\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM \"bar\".\"baz\"", "SELECT * FROM \"bar\".\"baz\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM \"bar\".baz", "SELECT * FROM \"bar\".baz");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.\"baz\"", "SELECT * FROM bar.\"baz\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.baz", "SELECT * FROM bar.baz");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.baz.qux", "SELECT * FROM bar.baz.qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM \"bar\".baz.qux", "SELECT * FROM \"bar\".baz.qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.\"baz\".qux", "SELECT * FROM bar.\"baz\".qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.baz.\"qux\"", "SELECT * FROM bar.baz.\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM \"bar\".\"baz\".qux",
        "SELECT * FROM \"bar\".\"baz\".qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.\"baz\".\"qux\"",
        "SELECT * FROM bar.\"baz\".\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM \"bar\".baz.\"qux\"",
        "SELECT * FROM \"bar\".baz.\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM \"bar\".\"baz\".\"qux\"",
        "SELECT * FROM \"bar\".\"baz\".\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM \"bar.baz.qux\"", "SELECT * FROM \"bar.baz.qux\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT \"*\" FROM bar", "SELECT \"*\" FROM bar");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT \"\"\"*\"\"\" FROM bar", "SELECT \"\"\"*\"\"\" FROM bar");
  }

  @Test
  public void testGetViewSqlBackTicked() throws UserException {
    setupGetViewSqlResources(Quoting.BACK_TICK);
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE VIEW foo\n AS\nSELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE VIEW\n foo AS SELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE \nVIEW foo AS\n\t SELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE \nVIEW foo AS\n\t SELECT\n* FROM \nbar", "SELECT\n* FROM \nbar");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM `bar`", "SELECT * FROM \"bar\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM `bar`.`baz`", "SELECT * FROM \"bar\".\"baz\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM `bar`.baz", "SELECT * FROM \"bar\".baz");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.`baz`", "SELECT * FROM bar.\"baz\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.baz", "SELECT * FROM bar.baz");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.baz.qux", "SELECT * FROM bar.baz.qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM `bar`.baz.qux", "SELECT * FROM \"bar\".baz.qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.`baz`.qux", "SELECT * FROM bar.\"baz\".qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.baz.`qux`", "SELECT * FROM bar.baz.\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM `bar`.`baz`.qux", "SELECT * FROM \"bar\".\"baz\".qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.`baz`.`qux`", "SELECT * FROM bar.\"baz\".\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM `bar`.baz.`qux`", "SELECT * FROM \"bar\".baz.\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM `bar`.`baz`.`qux`",
        "SELECT * FROM \"bar\".\"baz\".\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM `bar.baz.qux`", "SELECT * FROM \"bar.baz.qux\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT `*` FROM bar", "SELECT \"*\" FROM bar");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT `\"*\"` FROM bar", "SELECT \"\"\"*\"\"\" FROM bar");
  }

  @Test
  public void testGetViewSqlBracketed() throws UserException {
    setupGetViewSqlResources(Quoting.BRACKET);
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE VIEW foo\n AS\nSELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE VIEW\n foo AS SELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE \nVIEW foo AS\n\t SELECT * FROM bar", "SELECT * FROM bar");
    runTestGetViewSql("CREATE \nVIEW foo AS\n\t SELECT\n* FROM \nbar", "SELECT\n* FROM \nbar");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM [bar]", "SELECT * FROM \"bar\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM [bar].[baz]", "SELECT * FROM \"bar\".\"baz\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM [bar].baz", "SELECT * FROM \"bar\".baz");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.[baz]", "SELECT * FROM bar.\"baz\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.baz", "SELECT * FROM bar.baz");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT * FROM bar.baz.qux", "SELECT * FROM bar.baz.qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM [bar].baz.qux", "SELECT * FROM \"bar\".baz.qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.[baz].qux", "SELECT * FROM bar.\"baz\".qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.baz.[qux]", "SELECT * FROM bar.baz.\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM [bar].[baz].qux", "SELECT * FROM \"bar\".\"baz\".qux");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM bar.[baz].[qux]", "SELECT * FROM bar.\"baz\".\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM [bar].baz.[qux]", "SELECT * FROM \"bar\".baz.\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM [bar].[baz].[qux]",
        "SELECT * FROM \"bar\".\"baz\".\"qux\"");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT * FROM [bar.baz.qux]", "SELECT * FROM \"bar.baz.qux\"");
    runTestGetViewSql("CREATE VIEW foo AS\nSELECT [*] FROM bar", "SELECT \"*\" FROM bar");
    runTestGetViewSql(
        "CREATE VIEW foo AS\nSELECT [\"*\"] FROM bar", "SELECT \"\"\"*\"\"\" FROM bar");
  }

  @Test
  public void createViewWithoutALTERPrivilege() throws Exception {
    setupCreateViewHandler();
    doThrow(UserException.validationError().message("permission denied").buildSilently())
        .when(catalog)
        .validatePrivilege(RESOLVED_NAMESPACE_VIEW_KEY, SqlGrant.Privilege.ALTER);
    when(config.getContext().getSession().getSessionVersionForSource(DEFAULT_SOURCE_NAME))
        .thenReturn(sessionVersion);
    doReturn("").when(createViewHandler).getViewSql(default_input, "");
    doReturn(null)
        .when(createViewHandler)
        .validateTablesAndVersionContext(sqlNode, RESOLVED_NAMESPACE_VIEW_KEY, sessionVersion);
    doReturn(RESOLVED_NAMESPACE_VIEW_KEY).when(catalog).resolveSingle(VIEW_PATH_NS);
    assertThatThrownBy(() -> createViewHandler.toResult("", default_input))
        .isInstanceOf(UserException.class)
        .hasMessage("permission denied");
  }

  private void runTestGetViewSql(String sql, String expected) throws UserException {
    SqlNode sqlNode = parser.parse(sql);
    String result = createViewHandler.getViewSql((SqlCreateView) sqlNode, sql);
    assertThat(result).isEqualTo(expected);
  }

  private void setupGetViewSqlResources(Quoting quoting) {
    when(config.getContext()).thenReturn(context);
    when(context.getCatalog()).thenReturn(catalog);
    when(context.getConfig()).thenReturn(sabotConfig);
    when(context.getContextInformation()).thenReturn(contextInformation);
    when(context.getFunctionRegistry()).thenReturn(functions);
    when(context.getMaterializationProvider()).thenReturn(materializationProvider);
    when(context.getPlannerSettings()).thenReturn(settings);
    when(context.getSession()).thenReturn(userSession);
    when(context.getSubstitutionProviderFactory()).thenReturn(factory);
    when(factory.getSubstitutionProvider(
            any(SabotConfig.class), any(MaterializationList.class), any(OptionResolver.class)))
        .thenReturn(substitutionProvider);
    when(settings.getIdentifierMaxLength()).thenReturn(1024L);
    when(settings.getOptions()).thenReturn(options);
    when(settings.isFullNestedSchemaSupport()).thenReturn(true);
    when(settings.unwrap(PlannerSettings.class)).thenReturn(settings);
    when(settings.unwrap(CancelFlag.class)).thenReturn(null);
    when(userSession.getInitialQuoting()).thenReturn(quoting);
    when(userSession.supportFullyQualifiedProjections()).thenReturn(false);
    when(userSession.getCredentials()).thenReturn(userCredentials);

    parser =
        new SqlConverter(
            context.getPlannerSettings(),
            context.getOperatorTable(),
            context,
            context.getMaterializationProvider(),
            context.getFunctionRegistry(),
            context.getSession(),
            observer,
            context.getSubstitutionProviderFactory(),
            context.getConfig(),
            context.getScanResult(),
            context.getRelMetadataQuerySupplier());
    createViewHandler = spy(new CreateViewHandler(config));
  }
}
