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
package com.dremio.dac.obfuscate.visitor;

import com.dremio.exec.planner.sql.parser.SqlDeleteFromTable;
import com.dremio.exec.planner.sql.parser.SqlInsertTable;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.planner.sql.parser.SqlUpdateTable;
import java.util.function.Supplier;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;

/**
 * The main responsibility of this class is to call the appropriate {@code visit()} method inside
 * {@link ObfuscationRegistry#processSqlCall} basis the subclass type of {@link SqlCall}. Any
 * command which does not have a matching {@code visit()} function is delegated to the super class
 * of the caller visitor.
 */
public class ObfuscationRegistry {
  private static final BaseVisitor baseVisitor = new BaseVisitor();

  public static SqlNode obfuscate(SqlNodeList sqlNodeList) {
    return sqlNodeList.get(0).accept(baseVisitor);
  }

  static SqlNode processOtherSqlKind(SqlCall call, Supplier<SqlNode> defaultProcessor) {
    switch (call.getOperator().getName()) {
      case "OPTIMIZE":
        return baseVisitor.visit((SqlOptimize) call);
      default:
        return defaultProcessor.get();
    }
  }

  static <T extends SqlCall> SqlNode processSqlCall(T call, Supplier<SqlNode> defaultProcessor) {
    switch (call.getKind()) {
      case SELECT:
        return baseVisitor.visit((SqlSelect) call);
      case JOIN:
        return baseVisitor.visit((SqlJoin) call);
      case ORDER_BY:
        return baseVisitor.visit((SqlOrderBy) call);
      case WITH:
        return baseVisitor.visit((SqlWith) call);
      case WITH_ITEM:
        return baseVisitor.visit((SqlWithItem) call);
      case DELETE:
        return baseVisitor.visit((SqlDeleteFromTable) call);
      case INSERT:
        return baseVisitor.visit((SqlInsertTable) call);
      case MERGE:
        return baseVisitor.visit((SqlMerge) call);
      case UPDATE:
        return baseVisitor.visit((SqlUpdateTable) call);
      case OTHER:
        return processOtherSqlKind(call, defaultProcessor);
      default:
        return defaultProcessor.get();
    }
  }
}
