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
package com.dremio.service.autocomplete.parsing;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.context.AdditionalContext;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioRelMetadataQuery;
import com.dremio.exec.planner.sql.ConvertletTable;
import com.dremio.exec.planner.sql.DremioSqlConformance;
import com.dremio.exec.planner.sql.SqlValidatorImpl;
import com.dremio.exec.planner.sql.VersionedTableExpressionResolver;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.exec.context.ContextInformation;
import com.google.common.base.Preconditions;

/**
 * A parser that can convert to both SqlNode, which also performs node validation to ensure that converted SQL is correct.
 */
public final class ValidatingParser extends SqlNodeParser {
  // Mocks required for validator and expression resolver
  private static final RexBuilder REX_BUILDER = new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE);
  private static final RelOptTable.ViewExpander VIEW_EXPANDER = (a, b, c, d) -> {throw new RuntimeException("View Expansion not supported.");};
  private static final ConvertletTable CONVERTLET_TABLE = new ConvertletTable(new ContextInfoImpl("user"), false);

  private final SqlValidator validator;
  private final SqlToRelConverter sqlToRelConverter;
  private final VersionedTableExpressionResolver resolver;

  private <T extends SqlValidatorCatalogReader & Prepare.CatalogReader> ValidatingParser(
    SqlOperatorTable operatorTable,
    T catalogReader) {
    Preconditions.checkNotNull(operatorTable);
    Preconditions.checkNotNull(catalogReader);

    validator = new SqlValidatorImpl(
      new SqlValidatorImpl.FlattenOpCounter(),
      operatorTable,
      catalogReader,
      JavaTypeFactoryImpl.INSTANCE,
      DremioSqlConformance.INSTANCE,
      null);
    validator.setIdentifierExpansion(true);
   RelOptCluster cluster = RelOptCluster.create(
      new HepPlanner(new HepProgramBuilder().build()),
      new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE));
    cluster.setMetadataQuery(DremioRelMetadataQuery.QUERY_SUPPLIER);
    this.sqlToRelConverter = new SqlToRelConverter(VIEW_EXPANDER, validator, catalogReader,
      cluster, CONVERTLET_TABLE, SqlToRelConverter.Config.DEFAULT);
    this.resolver = new VersionedTableExpressionResolver(validator, REX_BUILDER);
  }

  @Override
  public SqlNode parseWithException(String sql) {
    SqlNode node = BaseSqlNodeParser.INSTANCE.parse(sql);
    // Resolve version context expressions to literals
    resolver.resolve(sqlToRelConverter, node);
    return validator.validate(node);
  }

  public static ValidatingParser create(
    final SqlOperatorTable operatorTable,
    final SimpleCatalog<?> catalog) {
    final DremioCatalogReader catalogReader = new DremioCatalogReader(
      catalog,
      JavaTypeFactoryImpl.INSTANCE);
    final SqlOperatorTable chainedOperatorTable = ChainedSqlOperatorTable.of(
      operatorTable,
      catalogReader);
    final ValidatingParser parser = new ValidatingParser(
      chainedOperatorTable,
      catalogReader);

    return parser;
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
    public void registerAdditionalInfo(AdditionalContext object) {
    }

    @Override
    public <T extends AdditionalContext> T getAdditionalInfo(Class<T> clazz) {
      return null;
    }

    @Override
    public boolean isPlanCacheable() {
      return true;
    }

    @Override
    public void setPlanCacheable(boolean isplancacheable) {}
  }
}
