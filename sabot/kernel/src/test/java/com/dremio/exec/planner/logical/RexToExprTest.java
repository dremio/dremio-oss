/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.logical;

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.google.common.collect.ImmutableList;

public class RexToExprTest {

  /* Method checks if we raise the appropriate error while dealing with RexNode that cannot be converted to
   * equivalent Dremio expressions
   */
  @Test
  public void testUnsupportedRexNode() {
    try {
      // Create the data type factory.
      RelDataTypeFactory relFactory = SqlTypeFactoryImpl.INSTANCE;
      // Create the rex builder
      RexBuilder rex = new DremioRexBuilder(relFactory);
      RelDataType anyType = relFactory.createSqlType(SqlTypeName.ANY);
      List<RexNode> emptyList = new LinkedList<>();
      ImmutableList<RexFieldCollation> e = ImmutableList.copyOf(new RexFieldCollation[0]);

      // create a dummy RexOver object.
      RexNode window = rex.makeOver(anyType, SqlStdOperatorTable.AVG, emptyList, emptyList, e, null, null, true,
          false, false);
      RexToExpr.toExpr(null, null, null, window);
    } catch (UserException e) {
      if (e.getMessage().contains(RexToExpr.UNSUPPORTED_REX_NODE_ERROR)) {
        // got expected error return
        return;
      }
      Assert.fail("Hit exception with unexpected error message");
    }

    Assert.fail("Failed to raise the expected exception");
  }
}
