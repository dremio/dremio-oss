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
package com.dremio.exec.expr.fn.impl;

import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.ValueExpressions.DateExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.DateMilliHolder;

/**
 * Casting a long to DATE. SQL standard doesn't allow this, but we need this because we store the date constant as
 * bigint and during fragment construction in
 * {@link ExpressionStringBuilder#visitDateConstant(DateExpression, StringBuilder)}
 */
@SuppressWarnings("unused")
@FunctionTemplate(names = {"castDATE"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls= NullHandling.NULL_IF_NULL)
public class CastBigIntDate implements SimpleFunction {

  @Param
  BigIntHolder in;
  @Output
  DateMilliHolder out;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
    out.value = in.value;
  }
}
