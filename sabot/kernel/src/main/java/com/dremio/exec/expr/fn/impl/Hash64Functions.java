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
 * Class contains hash64 function definitions for different data types.
 */
public class Hash64Functions {
  @FunctionTemplate(name = "hash64", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableFloatHash implements SimpleFunction {

    @Param
    NullableFloat4Holder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash64", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDoubleHash implements SimpleFunction {

    @Param
    NullableFloat8Holder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }

  @FunctionTemplate(names = {"hash64", "hash64AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableVarBinaryHash implements SimpleFunction {

    @Param
    NullableVarBinaryHolder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.start, in.end, in.buffer, 0);
      }
    }
  }

  @FunctionTemplate(names = {"hash64", "hash64AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableVarCharHash implements SimpleFunction {

    @Param
    NullableVarCharHolder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.start, in.end, in.buffer, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash64", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableBigIntHash implements SimpleFunction {

    @Param
    NullableBigIntHolder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash64", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableIntHash implements SimpleFunction {
    @Param
    NullableIntHolder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }

  @FunctionTemplate(names = {"hash64", "hash64AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDateHash implements SimpleFunction {
    @Param
    NullableDateMilliHolder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }

  @FunctionTemplate(names = {"hash64", "hash64AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableTimeStampHash implements SimpleFunction {
    @Param
    NullableTimeStampMilliHolder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }

  @FunctionTemplate(names = {"hash64", "hash64AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableTimeHash implements SimpleFunction {
    @Param
    NullableTimeMilliHolder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }

  @FunctionTemplate(names = {"hash64", "hash64AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimalHash implements SimpleFunction {
    @Param
    NullableDecimalHolder in;
    @Output NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.start, in.start + 16, in
          .buffer, 0);
      }
    }
  }

  @FunctionTemplate(names = {"hash64", "hash64AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableBitHash implements SimpleFunction {

    @Param
    NullableBitHolder in;
    @Output
    NullableBigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }
}
