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

import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

/*
 * Class contains hash32 function definitions for different data types.
 */
public class Hash32FunctionsWithSeed {
  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableFloatHash implements SimpleFunction {

    @Param NullableFloat4Holder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableDoubleHash implements SimpleFunction {

    @Param NullableFloat8Holder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableVarBinaryHash implements SimpleFunction {

    @Param NullableVarBinaryHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.start, in.end, in.buffer, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableVarCharHash implements SimpleFunction {

    @Param NullableVarCharHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.start, in.end, in.buffer, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableBigIntHash implements SimpleFunction {

    @Param NullableBigIntHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      }
      else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableIntHash implements SimpleFunction {
    @Param NullableIntHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      }
      else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDateHash implements SimpleFunction {
    @Param  NullableDateMilliHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableTimeStampHash implements SimpleFunction {
    @Param  NullableTimeStampMilliHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableTimeHash implements SimpleFunction {
    @Param  NullableTimeMilliHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimalHash implements SimpleFunction {
    @Param
    NullableDecimalHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start + 16), in
          .buffer, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableBitHash implements SimpleFunction {

    @Param NullableBitHolder in;
    @Param IntHolder seed;
    @Output NullableIntHolder out;


    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash32(in.value, seed.value);
      }
    }
  }
}
