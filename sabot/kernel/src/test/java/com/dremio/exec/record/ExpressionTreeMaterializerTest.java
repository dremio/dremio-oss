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
package com.dremio.exec.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.exec.ExecTest;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import org.junit.Test;
import org.mockito.Mockito;

public class ExpressionTreeMaterializerTest extends ExecTest {

  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExpressionTreeMaterializerTest.class);

  final BatchSchema schema = Mockito.mock(BatchSchema.class);

  FunctionImplementationRegistry registry = FUNCTIONS();

  @Test
  public void testMaterializingConstantTree() throws SchemaChangeException {
    ErrorCollector ec = new ErrorCollectorImpl();
    LogicalExpression expr =
        ExpressionTreeMaterializer.materialize(
            new ValueExpressions.LongExpression(1L), schema, ec, registry);
    assertTrue(expr instanceof ValueExpressions.LongExpression);
    assertEquals(1L, ValueExpressions.LongExpression.class.cast(expr).getLong());
    assertFalse(ec.hasErrors());
  }
}
