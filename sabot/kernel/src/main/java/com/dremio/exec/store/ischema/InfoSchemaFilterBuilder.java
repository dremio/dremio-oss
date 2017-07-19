/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.ischema;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CastExpression;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.ValueExpressions.QuotedString;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.store.ischema.InfoSchemaFilter.ConstantExprNode;
import com.dremio.exec.store.ischema.InfoSchemaFilter.FieldExprNode;
import com.dremio.exec.store.ischema.InfoSchemaFilter.FunctionExprNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import static com.dremio.exec.store.ischema.InfoSchemaConstants.*;

import java.util.List;

/**
 * Builds a InfoSchemaFilter out of the Filter condition. Currently we look only for certain conditions. Mainly
 * conditions involving columns "CATALOG_NAME, "TABLE_NAME", "SCHEMA_NAME", "TABLE_SCHEMA" and "COLUMN_NAME", and
 * functions EQUAL, NOT EQUAL, LIKE, OR and AND.
 */
public class InfoSchemaFilterBuilder extends AbstractExprVisitor<ExprNode, Void, RuntimeException> {
  private final LogicalExpression filter;

  private boolean isAllExpressionsConverted = true;

  public InfoSchemaFilterBuilder(LogicalExpression filter) {
    this.filter = filter;
  }

  public InfoSchemaFilter build() {
    ExprNode exprRoot = filter.accept(this, null);
    if (exprRoot != null) {
      return new InfoSchemaFilter(exprRoot);
    }

    return null;
  }

  public boolean isAllExpressionsConverted() {
    return isAllExpressionsConverted;
  }

  @Override
  public ExprNode visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    final String funcName = call.getName().toLowerCase();
    switch(funcName) {
      case "equal":
      case "not equal":
      case "notequal":
      case "not_equal": {
        final ExprNode col = call.args.get(0).accept(this, value);
        final ExprNode constant = call.args.get(1).accept(this, value);

        if (col instanceof FieldExprNode && constant instanceof ConstantExprNode) {
          return new FunctionExprNode(funcName, ImmutableList.of(col, constant));
        }
        break;
      }

      case "like": {
        final ExprNode col = call.args.get(0).accept(this, value);
        final ExprNode pattern = call.args.get(1).accept(this, value);
        final ExprNode escape = call.args.size() > 2 ? call.args.get(2).accept(this, value) : null;

        if (col instanceof FieldExprNode && pattern instanceof ConstantExprNode &&
            (escape == null || escape instanceof ConstantExprNode)) {
          return new FunctionExprNode(funcName,
              escape == null ? ImmutableList.of(col, pattern) : ImmutableList.of(col, pattern, escape));
        }
        break;
      }

      case "booleanand": {
        List<ExprNode> args = Lists.newArrayList();
        for(LogicalExpression arg : call.args) {
          ExprNode exprNode = arg.accept(this, value);
          if (exprNode != null && exprNode instanceof FunctionExprNode) {
            args.add(exprNode);
          }
        }
        if (args.size() > 0) {
          return new FunctionExprNode(funcName, args);
        }

        return visitUnknown(call, value);
      }

      case "booleanor": {
        List<ExprNode> args = Lists.newArrayList();
        for(LogicalExpression arg : call.args) {
          ExprNode exprNode = arg.accept(this, value);
          if (exprNode != null && exprNode instanceof FunctionExprNode) {
            args.add(exprNode);
          } else {
            return visitUnknown(call, value);
          }
        }

        if (args.size() > 0) {
          return new FunctionExprNode(funcName, args);
        }

        visitUnknown(call, value);
      }
    }

    return visitUnknown(call, value);
  }

  @Override
  public ExprNode visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public ExprNode visitCastExpression(CastExpression e, Void value) throws RuntimeException {
    if (e.getInput() instanceof FieldReference) {
      FieldReference fieldRef = (FieldReference) e.getInput();
      String field = fieldRef.getAsUnescapedPath().toUpperCase();
      if (field.equals(CATS_COL_CATALOG_NAME)
          || field.equals(SCHS_COL_SCHEMA_NAME)
          || field.equals(SHRD_COL_TABLE_NAME)
          || field.equals(SHRD_COL_TABLE_SCHEMA)
          || field.equals(COLS_COL_COLUMN_NAME)) {
        return new FieldExprNode(field);
      }
    }

    return visitUnknown(e, value);
  }

  @Override
  public ExprNode visitQuotedStringConstant(QuotedString e, Void value) throws RuntimeException {
    return new ConstantExprNode(e.value);
  }

  @Override
  public ExprNode visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {
    String field = path.getAsUnescapedPath().toUpperCase();
    if (field.equals(CATS_COL_CATALOG_NAME)
        || field.equals(SCHS_COL_SCHEMA_NAME)
        || field.equals(SHRD_COL_TABLE_NAME)
        || field.equals(SHRD_COL_TABLE_SCHEMA)
        || field.equals(COLS_COL_COLUMN_NAME)) {
      return new FieldExprNode(field);
    }

    return visitUnknown(path, value);
  }

  @Override
  public ExprNode visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    isAllExpressionsConverted = false;
    return null;
  }
}
