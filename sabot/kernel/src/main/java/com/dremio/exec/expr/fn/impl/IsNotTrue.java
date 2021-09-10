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

import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

public class IsNotTrue {

  @FunctionTemplate(names = {"isnottrue", "is not true"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Optional implements SimpleFunction {

    @Param NullableBitHolder in;
    @Output NullableBitHolder out;

    public void setup() { }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 1;
      } else {
        out.value = (in.value == 0 ? 1 : 0);
      }
    }
  }

  @FunctionTemplate(names = {"isnottrue", "is not true"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class OptionalInt implements SimpleFunction {

    @Param NullableIntHolder in;
    @Output NullableBitHolder out;

    public void setup() { }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 1 && in.value != 0) {
        out.value = 0;
      } else {
        out.value = 1;
      }
    }
  }

  @FunctionTemplate(names = {"isnottrue", "is not true"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class OptionalLong implements SimpleFunction {

    @Param NullableBigIntHolder in;
    @Output NullableBitHolder out;

    public void setup() { }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 1 && in.value != 0L) {
        out.value = 0;
      } else {
        out.value = 1;
      }
    }
  }
}
