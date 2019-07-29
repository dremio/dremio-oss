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
package com.dremio.exec.expr.fn.impl.conv;


import org.apache.arrow.vector.complex.writer.BaseWriter;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.fn.OutputDerivation.Dummy;

/**
 * This and {@link DummyConvertTo} class merely act as a placeholder so that Optiq
 * allows the 'flatten()' function in SQL.
 */
@FunctionTemplate(name = "flatten", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, derivation = Dummy.class)
@SuppressWarnings("unused") // found through classpath search
public class DummyFlatten implements SimpleFunction {

  @Output BaseWriter.ComplexWriter out;

  @Override
  public void setup() { }

  @Override
  public void eval() { }
}
