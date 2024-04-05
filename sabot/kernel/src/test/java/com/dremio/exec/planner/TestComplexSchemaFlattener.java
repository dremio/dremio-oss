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
package com.dremio.exec.planner;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import com.dremio.exec.planner.common.ComplexSchemaFlattener;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

/** Test for complex schema flattener */
public class TestComplexSchemaFlattener {
  private static final RelDataTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  private List<SqlTypeName> sqlTypeNameList;
  private RelDataType rowType;

  @Before
  public void setup() {
    sqlTypeNameList =
        asList(
            SqlTypeName.VARCHAR,
            SqlTypeName.BIGINT,
            SqlTypeName.DOUBLE,
            SqlTypeName.INTEGER,
            SqlTypeName.BOOLEAN,
            SqlTypeName.FLOAT);

    RelDataTypeFactory.Builder builder = typeFactory.builder();

    List<RelDataType> typeList =
        ImmutableList.of(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            typeFactory.createSqlType(SqlTypeName.DOUBLE),
            typeFactory.createStructType(
                asList(
                    typeFactory.createSqlType(SqlTypeName.INTEGER),
                    typeFactory.createStructType(
                        asList(
                            typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                            typeFactory.createSqlType(SqlTypeName.FLOAT)),
                        asList("a3", "b3"))),
                asList("a2", "struct3")));
    List<String> names = ImmutableList.of("a1", "b1", "c1", "struct2");

    builder.add(
        new RelDataTypeFieldImpl("struct1", 0, typeFactory.createStructType(typeList, names)));
    rowType = builder.build();
  }

  @Test
  public void testComplexSchemaFlattenerWithJoiner() {
    List<String> leafNodesWithoutJoiner = asList("a1", "b1", "c1", "a2", "a3", "b3");
    ComplexSchemaFlattener flattener = new ComplexSchemaFlattener(rexBuilder, null);

    flattener.flatten(rowType);

    assertEquals(
        sqlTypeNameList,
        flattener.getTypeList().stream()
            .map(RelDataType::getSqlTypeName)
            .collect(Collectors.toList()));
    assertEquals(leafNodesWithoutJoiner, flattener.getFields());
  }

  @Test
  public void testComplexSchemaFlattenerWithoutJoiner() {
    List<String> leafNodesWithJoiner =
        asList("struct1_a1", "struct1_b1", "struct1_c1", "a2", "a3", "b3");
    ComplexSchemaFlattener flattener = new ComplexSchemaFlattener(rexBuilder, "_");

    flattener.flatten(rowType);

    assertEquals(
        sqlTypeNameList,
        flattener.getTypeList().stream()
            .map(RelDataType::getSqlTypeName)
            .collect(Collectors.toList()));
    assertEquals(leafNodesWithJoiner, flattener.getFields());
  }
}
