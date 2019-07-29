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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.arrow.vector.IntVector;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.exceptions.ExpressionParsingException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.parser.ExprLexer;
import com.dremio.common.expression.parser.ExprParser;
import com.dremio.common.expression.parser.ExprParser.parse_return;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.ExecTest;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.CompilationOptions;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.project.Projector;

public class ExpressionTest extends ExecTest {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTest.class);

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private VectorAccessible getBatch(MinorType type){
    final IntVector vector = new IntVector("result", RootAllocatorFactory.newRoot(DEFAULT_SABOT_CONFIG));
    VectorWrapper<IntVector> wrapper = Mockito.mock(VectorWrapper.class);
    when(wrapper.getValueVector()).thenReturn(vector);

    final TypedFieldId tfid = new TypedFieldId(CompleteType.fromMinorType(type), false, 0);
    VectorAccessible batch = Mockito.mock(VectorAccessible.class);
    when(batch.getValueVectorId(any(SchemaPath.class))).thenReturn(tfid);
    when(batch.getValueAccessorById(any(Class.class), any(int[].class))).thenReturn((VectorWrapper) wrapper);
    when(batch.getSchema()).thenReturn(BatchSchema.newBuilder().addField(MajorTypeHelper.getFieldForNameAndMajorType("result", Types.optional(type))).setSelectionVectorMode(SelectionVectorMode.NONE).build());
    return batch;
  }

  private final FunctionImplementationRegistry registry = FUNCTIONS();

  @Test
  public void testBasicExpression() throws Exception {
    getExpressionCode("if(true) then 1 else 0 end");
  }

  @Test
  public void testExprParseUpperExponent() throws Exception {
    getExpressionCode("multiply(`result`, 1.0E-4)");
  }

  @Test
  public void testExprParseLowerExponent() throws Exception {
    getExpressionCode("multiply(`result`, 1.0e-4)");
  }

  @Test
  public void testSpecial() throws Exception {
    System.out.println(getExpressionCode("1 + 1"));
  }

  @Test
  public void testSchemaExpression() throws Exception {
    getExpressionCode("1 + result");

  }

  @Test(expected = ExpressionParsingException.class)
  public void testExprParseError() throws Exception {
    getExpressionCode("less than(1, 2)");
  }

  @Test
  public void testExprParseNoError() throws Exception {
    getExpressionCode("equal(1, 2)");
  }

  @Test
  public void testTimeDiffExpr() throws Exception {
    getExpressionCode("timestampdiffSecond(castTIMESTAMP(castDATE(1486080000000l) ) , " + "castTIMESTAMP(castDATE(1486166400000l) ) )");
  }

  // HELPER METHODS //

  private LogicalExpression parseExpr(String expr) throws RecognitionException {
    final ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final ExprParser parser = new ExprParser(tokens);
    parse_return ret = parser.parse();
    return ret.e;
  }

  private String getExpressionCode(String expression) throws Exception {
    VectorAccessible batch = getBatch(MinorType.BIGINT);
    final LogicalExpression expr = parseExpr(expression);
    final ErrorCollector error = new ErrorCollectorImpl();
    final LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, batch.getSchema(), error, registry);
    if (error.getErrorCount() != 0) {
      System.err.println(String.format("Failure while materializing expression [%s].  Errors: %s", expression, error));
      assertEquals(0, error.getErrorCount());
    }

    CompilationOptions compilationOptions = mock(CompilationOptions.class);
    when(compilationOptions.getNewMethodThreshold()).thenReturn(100);
    FunctionContext mockFunctionContext = mock(FunctionContext.class);
    when(mockFunctionContext.getCompilationOptions()).thenReturn(compilationOptions);
    final ClassGenerator<Projector> cg = CodeGenerator.get(Projector.TEMPLATE_DEFINITION, null, mockFunctionContext).getRoot();
    cg.addExpr(new ValueVectorWriteExpression(new TypedFieldId(materializedExpr.getCompleteType(), -1), materializedExpr));
    return cg.getCodeGenerator().generateAndGet();
  }
}
