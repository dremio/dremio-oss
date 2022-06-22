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
package com.dremio.exec.store.iceberg.deletes;

import java.util.List;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetFilterIface;
import com.google.common.collect.ImmutableList;


/**
 * An interface for creating Parquet filters used with Iceberg delete files.
 */
public interface ParquetDeleteFileFilterCreator {

  List<ParquetFilterCondition> createFilePathFilter(String startFilePath, String endFilePath);

  ParquetDeleteFileFilterCreator DEFAULT = new ParquetDeleteFileFilterCreator() {

    private final ParquetFilterIface EXACT_FILTER = new ParquetFilterIface() {
      @Override
      public boolean exact() {
        return true;
      }
    };

    @Override
    public List<ParquetFilterCondition> createFilePathFilter(String startFilePath, String endFilePath) {
      SchemaPath filePathColumn = SchemaPath.getSimplePath(PositionalDeleteFileReader.FILE_PATH_COLUMN);
      LogicalExpression val = new FieldReference(filePathColumn);
      LogicalExpression startExpr = ValueExpressions.getChar(startFilePath);
      LogicalExpression arg1 = FunctionCallFactory.createExpression("less_than_or_equal_to", startExpr, val);
      LogicalExpression endExpr = ValueExpressions.getChar(endFilePath);
      LogicalExpression arg2 = FunctionCallFactory.createExpression("greater_than_or_equal_to", endExpr, val);
      LogicalExpression expr = FunctionCallFactory.createBooleanOperator("booleanAnd", arg1, arg2);

      ParquetFilterCondition condition = new ParquetFilterCondition(filePathColumn, EXACT_FILTER, expr, 0);
      return ImmutableList.of(condition);
    }
  };
}
