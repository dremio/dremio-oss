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
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

/*
 * Class contains hash64 function definitions for different data types.
 *
 * NOTE: These functions are used internally by Dremio to perform hash distribution and in hash join. For
 * numeric data types we would like to apply implicit casts in the join method however for this to work
 * as expected we would need to hash the same value represented in different data types (int, bigint, float etc)
 * to hash to the same node, this is why we cast all numeric values to double before performing the actual hash.
 */
public class Hash64AsDouble {
  @FunctionTemplate(name = "hash64AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableFloatHash implements SimpleFunction {

    @Param
    NullableFloat4Holder in;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64((double) in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash64AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDoubleHash implements SimpleFunction {

    @Param
    NullableFloat8Holder in;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64(in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash64AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableBigIntHash implements SimpleFunction {

    @Param
    NullableBigIntHolder in;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64((double) in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash64AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableIntHash implements SimpleFunction {
    @Param
    NullableIntHolder in;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = com.dremio.common.expression.fn.impl.HashHelper.hash64((double) in.value, 0);
      }
    }
  }

}
