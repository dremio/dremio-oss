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
package com.dremio.exec.catalog.udf;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.store.sys.udf.FunctionOperatorTable;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.google.common.collect.ImmutableList;

public class SqlUserDefinedFunctionExpanderRuleTest {

  @Test
  public void convertCall() {
    UserDefinedFunction userDefinedFunction =
      new UserDefinedFunction("foo", "SELECT 1", CompleteType.INT, ImmutableList.of());

    CatalogIdentity catalogIdentity = mock(CatalogIdentity.class);

    DremioScalarUserDefinedFunction scalarFunction =
      new DremioScalarUserDefinedFunction(catalogIdentity, userDefinedFunction);

    SqlUserDefinedFunction sqlUserDefinedFunction = mock(SqlUserDefinedFunction.class);
    when(sqlUserDefinedFunction.getFunction()).thenReturn(scalarFunction);

    SqlCall sqlCall = mock(SqlCall.class);
    when(sqlCall.getOperator()).thenReturn(sqlUserDefinedFunction);

    Subject subject = new Subject();

    when(subject.sqlSubQueryConverter.validateAndConvertFunction(Mockito.eq(subject.sqlNode), any()))
      .thenReturn(subject.rexBuilder.makeLiteral(1,
        subject.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));

    SqlRexContext sqlRexContext = mock(SqlRexContext.class);
    when(sqlRexContext.getRexBuilder()).thenReturn(subject.rexBuilder);


    //TEST
    RexNode result = subject.sqlUserDefinedFunctionExpanderRule.convertCall(sqlRexContext, sqlCall);

    //ASSERT
    Assert.assertEquals("CAST(1):INTEGER", result.toString());
    verify(subject.sqlSubQueryConverterBuilder, times(1))
      .withSchemaPath(ImmutableList.of());
    verify(subject.sqlSubQueryConverterBuilder, times(1))
      .withUser(catalogIdentity);
    verify(subject.sqlSubQueryConverterBuilder, times(1))
      .withContextualSqlOperatorTable(new FunctionOperatorTable(ImmutableList.of()));
  }
}

class Subject {
  final RexBuilder rexBuilder = new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE);
  final RelOptCluster relOptCluster = mock(RelOptCluster.class);
  final SqlConverter sqlConverter = mock(SqlConverter.class);
  final SqlValidatorAndToRelContext sqlSubQueryConverter = mock(SqlValidatorAndToRelContext.class);
  final SqlValidatorAndToRelContext.Builder sqlSubQueryConverterBuilder = mock(SqlValidatorAndToRelContext.Builder.class);
  final SqlNode sqlNode = mock(SqlNode.class);
  final SqlNodeList sqlNodeList = mock(SqlNodeList.class);
  final SqlSelect sqlSelect = createSqlSelect(sqlNodeList);

  final SqlUserDefinedFunctionExpanderRule sqlUserDefinedFunctionExpanderRule =
    new SqlUserDefinedFunctionExpanderRule(() -> sqlSubQueryConverterBuilder);

  public Subject() {
    when(sqlSubQueryConverterBuilder.withSchemaPath(any())).thenReturn(sqlSubQueryConverterBuilder);
    when(sqlSubQueryConverterBuilder.withUser(any())).thenReturn(sqlSubQueryConverterBuilder);
    when(sqlSubQueryConverterBuilder.withContextualSqlOperatorTable(any())).thenReturn(sqlSubQueryConverterBuilder);
    when(sqlSubQueryConverterBuilder.build()).thenReturn(sqlSubQueryConverter);

    when(sqlSubQueryConverter.getSqlConverter()).thenReturn(sqlConverter);

    when(sqlConverter.getCluster()).thenReturn(relOptCluster);
    when(sqlConverter.parse(any())).thenReturn(sqlSelect);

    when(relOptCluster.getRexBuilder()).thenReturn(rexBuilder);

    when(sqlNodeList.size()).thenReturn(1);
    when(sqlNodeList.get(0)).thenReturn(sqlNode);
  }

  private static SqlSelect createSqlSelect(SqlNodeList sqlNodeList) {
    return new SqlSelect(SqlParserPos.ZERO, null, sqlNodeList,
      null, null, null, null, null, null,null, null, null, null);
  }
}
