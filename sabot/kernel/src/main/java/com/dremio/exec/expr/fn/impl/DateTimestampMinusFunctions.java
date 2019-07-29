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

import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

public class DateTimestampMinusFunctions {

  @FunctionTemplate(names = {"date_diff", "subtract", "date_sub"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class DateDiff implements SimpleFunction {

    @Param DateMilliHolder input1;
    @Param DateMilliHolder input2;
    @Output
    IntervalDayHolder out;

    public void setup() {
    }

    public void eval() {
      out.days =  (int) ((input1.value - input2.value) / org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
      out.milliseconds = 0;
    }
  }

  @FunctionTemplate(names = {"date_diff", "subtract", "date_sub"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class TimestampDiff implements SimpleFunction {

    @Param TimeStampMilliHolder input1;
    @Param TimeStampMilliHolder input2;
    @Output
    IntervalDayHolder out;

    public void setup() {
    }

    public void eval() {
      long difference = (input1.value - input2.value);
      out.milliseconds = (int) (difference % org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
      out.days = (int) (difference / org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
    }
  }
}
