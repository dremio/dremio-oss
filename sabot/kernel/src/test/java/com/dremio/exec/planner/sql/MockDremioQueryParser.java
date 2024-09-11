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
package com.dremio.exec.planner.sql;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.context.AdditionalContext;
import com.dremio.exec.ops.DelegatingPlannerCatalog;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioRelMetadataQuery;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.ContextInformation;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/**
 * A simplified way of parsing queries in dremio useful for testing purposes. Can only be used for a
 * single query.
 */
public final class MockDremioQueryParser {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MockDremioQueryParser.class);

  private final RelDataTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
  private final ParserConfig parserConfig = new ParserConfig(Quoting.DOUBLE_QUOTE, 1000, true);
  private final SqlValidator validator;
  private final RelOptCluster cluster;
  private final Prepare.CatalogReader catalogReader;
  private final SqlToRelConverter sqlToRelConverter;
  private final VersionedTableExpressionResolver resolver;

  private <T extends SqlValidatorCatalogReader & Prepare.CatalogReader> MockDremioQueryParser(
      SqlOperatorTable operatorTable, T catalogReader, ContextInformation contextInformation) {
    // Mocks required for validator and expression resolver
    final RexBuilder rexBuilder = new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE);
    final RelOptTable.ViewExpander viewExpander = mock(RelOptTable.ViewExpander.class);
    final ConvertletTable convertletTable = new ConvertletTable(false);

    this.catalogReader = catalogReader;
    if (catalogReader instanceof SqlOperatorTable) {
      operatorTable = new ChainedSqlOperatorTable(ImmutableList.of(operatorTable, catalogReader));
    }

    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(PlannerSettings.EXTENDED_ALIAS)).thenReturn(true);
    final SqlValidator.Config conf =
        SqlValidator.Config.DEFAULT
            .withSqlConformance(DremioSqlConformance.INSTANCE)
            .withAmbiguousColumnAllowed(
                optionManager.getOption(PlannerSettings.ALLOW_AMBIGUOUS_COLUMN));

    validator =
        new SqlValidatorImpl(
            new SqlValidatorImpl.FlattenOpCounter(),
            operatorTable,
            catalogReader,
            typeFactory,
            conf,
            optionManager);
    validator.setIdentifierExpansion(true);
    cluster =
        RelOptCluster.create(
            new HepPlanner(new HepProgramBuilder().build()), new DremioRexBuilder(typeFactory));
    cluster.setMetadataQuery(DremioRelMetadataQuery.QUERY_SUPPLIER);
    this.sqlToRelConverter =
        new SqlToRelConverter(
            viewExpander,
            validator,
            catalogReader,
            cluster,
            convertletTable,
            SqlToRelConverter.Config.DEFAULT);
    this.resolver = new VersionedTableExpressionResolver(validator, rexBuilder, contextInformation);
  }

  public MockDremioQueryParser(
      SqlOperatorTable operatorTable, SimpleCatalog<?> catalog, String user) {
    this(
        operatorTable,
        new DremioCatalogReader(DelegatingPlannerCatalog.newInstance(catalog)),
        new MockDremioQueryParser.ContextInfoImpl(user));
  }

  /**
   * Parse a sql string.
   *
   * @param sql Sql to parse.
   * @return The validated SqlTree tree.
   */
  public SqlNode parse(String sql) {
    try {
      SqlParser parser = SqlParser.create(sql, parserConfig);
      SqlNode node = parser.parseStmt();
      // Resolve version context expressions to literals
      resolver.resolve(sqlToRelConverter, node);
      SqlNode sqlNode = validator.validate(node);
      return sqlNode;
    } catch (SqlParseException e) {
      throw SqlExceptionHelper.parseError(sql, e).build(logger);
    }
  }

  public RelNode toRel(String query) {
    final SqlNode sqlNode = parse(query);
    return convertSqlNodeToRel(sqlNode);
  }

  /**
   * Get the rel from a previously parsed sql tree.
   *
   * @return The RelNode tree.
   */
  public RelNode convertSqlNodeToRel(SqlNode sqlNode) {
    final SqlToRelConverter.Config config =
        SqlToRelConverter.configBuilder()
            .withInSubQueryThreshold((int) 1024)
            .withTrimUnusedFields(true)
            .withExpand(true)
            .withConvertTableAccess(false)
            .build();
    final RelOptTable.ViewExpander expander =
        (a, b, c, d) -> {
          throw new RuntimeException("View Expansion not supported.");
        };
    final ConvertletTable convertletTable = new ConvertletTable(false);
    final SqlToRelConverter converter =
        new SqlToRelConverter(expander, validator, catalogReader, cluster, convertletTable, config);
    return converter.convertQuery(sqlNode, false, true).rel;
  }

  public RelOptCluster getCluster() {
    return cluster;
  }

  private static class ContextInfoImpl implements ContextInformation {

    private final String user;

    public ContextInfoImpl(String user) {
      super();
      this.user = user;
    }

    @Override
    public String getQueryUser() {
      return user;
    }

    @Override
    public String getCurrentDefaultSchema() {
      return null;
    }

    @Override
    public long getQueryStartTime() {
      return 0;
    }

    @Override
    public int getRootFragmentTimeZone() {
      return 0;
    }

    @Override
    public UserBitShared.QueryId getLastQueryId() {
      return null;
    }

    @Override
    public void registerAdditionalInfo(AdditionalContext object) {}

    @Override
    public <T extends AdditionalContext> T getAdditionalInfo(Class<T> clazz) {
      return null;
    }
  }
}
