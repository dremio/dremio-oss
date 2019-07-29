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
package com.dremio.exec.expr;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;

public interface ClassProducer {
  <T> CodeGenerator<T> createGenerator(TemplateClassDefinition<T> definition);
  LogicalExpression materializeWithBatchSchema(LogicalExpression expr, BatchSchema batchSchema);
  LogicalExpression materialize(LogicalExpression expr, VectorAccessible batch);
  LogicalExpression materializeAndAllowComplex(LogicalExpression expr, VectorAccessible batch);
  LogicalExpression materializeAndAllowComplex(ExpressionEvaluationOptions options, LogicalExpression expr, VectorAccessible batch);
  LogicalExpression addImplicitCast(LogicalExpression fromExpr, CompleteType toType);
  FunctionContext getFunctionContext();
  FunctionLookupContext getFunctionLookupContext();
}
