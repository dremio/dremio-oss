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
package com.dremio.exec.expr.fn.impl;

import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;

public class NumericFunctions {

  @FunctionTemplate(name = "isnumeric", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class NullableBigIntIsNumeric implements SimpleFunction {

    @Param
    NullableBigIntHolder in;
    @Output
    NullableBitHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = 1;
      }
      out.isSet = 1;
    }
  }

  @FunctionTemplate(name = "isnumeric", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class NullableIntIsNumeric implements SimpleFunction {

    @Param
    NullableIntHolder in;
    @Output
    NullableBitHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = 1;
      }
      out.isSet = 1;
    }
  }

  @FunctionTemplate(name = "isnumeric", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class NullableVarCharIsNumeric implements SimpleFunction {

    @Param
    NullableVarCharHolder in;
    @Output
    NullableBitHolder out;
    @Workspace
    java.util.regex.Pattern pattern;
    @Workspace
    java.util.regex.Matcher matcher;

    public void setup() {
      pattern = java.util.regex.Pattern.compile("[-+]?\\d+(\\.\\d+)?");
      matcher = pattern.matcher("");
    }

    public void eval() {

      if (in.isSet == 0) {
        out.value = 0;
      } else {
        String s = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end,
            in.buffer);
        out.value = matcher.reset(s).matches() ? 1 : 0;
      }
      out.isSet = 1;

    }
  }

}
