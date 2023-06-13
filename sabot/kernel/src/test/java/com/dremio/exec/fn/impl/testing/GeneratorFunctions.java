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
package com.dremio.exec.fn.impl.testing;

import java.util.Random;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;

import com.dremio.exec.ExecTest;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;

public class GeneratorFunctions extends ExecTest {

  public static final Random random = new Random(1234L);

  @FunctionTemplate(name = "increasingBigInt", isDeterministic = false)
  public static class IncreasingBigInt implements SimpleFunction {

    @Param BigIntHolder start;
    @Workspace long current;
    @Output NullableBigIntHolder out;

    @Override
    public void setup() {
      current = 0;
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.value = start.value + current++;
    }
  }

  @FunctionTemplate(name = "randomBigInt", isDeterministic = false)
  public static class RandomBigIntGauss implements SimpleFunction {

    @Param BigIntHolder range;
    @Output NullableBigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.value = (long)(com.dremio.exec.fn.impl.testing.GeneratorFunctions.random.nextGaussian() * range.value);
    }
  }

  @FunctionTemplate(name = "randomBigInt", isDeterministic = false)
  public static class RandomBigInt implements SimpleFunction {

    @Param BigIntHolder min;
    @Param BigIntHolder max;
    @Output NullableBigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.value = (long)(com.dremio.exec.fn.impl.testing.GeneratorFunctions.random.nextFloat() * (max.value - min.value) + min.value);
    }
  }

  @FunctionTemplate(name = "randomFloat8", isDeterministic = false)
  public static class RandomFloat8Gauss implements SimpleFunction {

    @Param BigIntHolder range;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.value = com.dremio.exec.fn.impl.testing.GeneratorFunctions.random.nextGaussian() * range.value;
    }
  }

  @FunctionTemplate(name = "randomFloat8", isDeterministic = false)
  public static class RandomFloat8 implements SimpleFunction {

    @Param BigIntHolder min;
    @Param BigIntHolder max;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.value = com.dremio.exec.fn.impl.testing.GeneratorFunctions.random.nextFloat() * (max.value - min.value) + min.value;
    }
  }
}
