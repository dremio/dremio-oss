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
package com.dremio.exec.planner.sql.handlers.query;

import com.dremio.common.collections.Tuple;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ScanLimitValidatorTest {
  private static final RelDataTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private static final RelDataType INTEGER =
      JavaTypeFactoryImpl.INSTANCE.createTypeWithNullability(
          JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.INTEGER), true);
  private final RelOptSchema schema;
  private final RelOptCluster cluster;

  public ScanLimitValidatorTest() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());

    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    HepPlanner planner =
        new HepPlanner(
            new HepProgramBuilder().build(), context, false, null, new DremioCost.Factory());
    schema = schemaFor(table("users", ImmutableList.of("a", "b", "c", "d", "e")));
    cluster = RelOptCluster.create(planner, rexBuilder);
  }

  @Test
  public void shouldDoNothingScanColumnsCountIsNotOverLimit() {
    RelBuilder builder = RelBuilder.proto(Contexts.empty()).create(cluster, schema);
    RelNode node =
        builder.scan("users").limit(100, 1000).project(rexBuilder.makeLiteral("test me")).build();

    Assertions.assertDoesNotThrow(() -> ScanLimitValidator.ensureLimit(node, 5));
  }

  @Test
  public void shouldThrowExceptionIfOverLimit() {
    RelBuilder builder = RelBuilder.proto(Contexts.empty()).create(cluster, schema);
    RelNode node =
        builder.scan("users").limit(100, 1000).project(rexBuilder.makeLiteral("test me")).build();

    Assertions.assertThrows(
        ValidationException.class, () -> ScanLimitValidator.ensureLimit(node, 4));
  }

  @Test
  public void shouldAccountForNestedColumnsWhenCalculatingLimit() {
    RelDataType structType =
        JavaTypeFactoryImpl.INSTANCE.createTypeWithNullability(
            JavaTypeFactoryImpl.INSTANCE.createStructType(
                ImmutableList.of(
                    JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.INTEGER),
                    JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.INTEGER),
                    JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.INTEGER)),
                ImmutableList.of("A", "B", "C")),
            true);

    RelOptSchema customSchema =
        schemaFor(
            table(
                "users",
                ImmutableList.of("a", "b", "nested"),
                ImmutableList.of(INTEGER, INTEGER, structType)));

    RelBuilder builder = RelBuilder.proto(Contexts.empty()).create(cluster, schema);
    RelNode node =
        builder.scan("users").limit(100, 1000).project(rexBuilder.makeLiteral("test me")).build();

    // We have 5 total including nested
    Assertions.assertThrows(
        ValidationException.class, () -> ScanLimitValidator.ensureLimit(node, 4));
  }

  private Tuple<String, RelDataType> table(String name, ImmutableList<String> fieldNames) {
    ImmutableList<RelDataType> types =
        fieldNames.stream().map(unused -> INTEGER).collect(ImmutableList.toImmutableList());
    return table(name, fieldNames, types);
  }

  private Tuple<String, RelDataType> table(
      String name, ImmutableList<String> fieldNames, ImmutableList<RelDataType> types) {
    return Tuple.of(name, typeFactory.createStructType(types, fieldNames));
  }

  private RelOptSchema schemaFor(Tuple<String, RelDataType>... tables) {
    Map<List<String>, RelOptTable> relTables = new HashMap<>();
    RelOptSchema schema =
        new RelOptSchema() {

          @Override
          public void registerRules(RelOptPlanner planner) throws Exception {}

          @Override
          public RelDataTypeFactory getTypeFactory() {
            return typeFactory;
          }

          @Override
          public RelOptTable getTableForMember(List<String> names) {
            return relTables.get(names);
          }
        };

    for (Tuple<String, RelDataType> table : tables) {
      relTables.put(
          Collections.singletonList(table.first),
          new RelOptAbstractTable(schema, table.first, table.second) {});
    }

    return schema;
  }
}
