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
package com.dremio.exec.expr.fn.impl;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.IntervalDayHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

/**
 * generated from CastIntervalExactNumeric.java IntervalDay BigInt IntervalDayExactNumeric
 */
@SuppressWarnings("unused")
@FunctionTemplate(name = "castBIGINT", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class CastIntervalDayToBigInt implements SimpleFunction {

  @Param
  IntervalDayHolder in;
  @Output
  BigIntHolder out;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
    out.value = (long) in.milliseconds + (long) in.days * (long) org.apache.arrow.vector.util.DateUtility.daysToStandardMillis;
  }
}
