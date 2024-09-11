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
package com.dremio.sabot.op.join.nlje;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.InputReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.exec.context.CompilationOptions;
import java.util.Arrays;
import org.apache.arrow.vector.IntVector;
import org.junit.Test;
import org.mockito.Mockito;

public class TestTemplate extends BaseTestOperator {

  @Test
  public void test() {
    CompilationOptions options = Mockito.mock(CompilationOptions.class);
    Mockito.when(options.enableOrOptimization()).thenReturn(true);
    Mockito.when(options.getNewMethodThreshold()).thenReturn(50);
    Mockito.when(options.getOrOptimizationThreshold()).thenReturn(50);
    Mockito.when(options.getOrOptimizationThresholdForGandiva()).thenReturn(50);

    VectorContainer probe = new VectorContainer();
    probe.add(new IntVector("a", getTestAllocator()));
    probe.buildSchema();

    VectorContainer build = new VectorContainer();
    build.addHyperList(Arrays.asList(new IntVector("a", getTestAllocator())));
    build.buildSchema(SelectionVectorMode.FOUR_BYTE);

    LogicalExpression e =
        new FunctionCall(
            "=",
            Arrays.asList(
                new InputReference(0, FieldReference.getSimplePath("a")),
                new InputReference(1, FieldReference.getSimplePath("a"))));

    MatchGenerator matchHolder =
        MatchGenerator.generate(
            e,
            testContext.newClassProducer(new BufferManagerImpl(getTestAllocator())),
            probe,
            build);
  }
}
