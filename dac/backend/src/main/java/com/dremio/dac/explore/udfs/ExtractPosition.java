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
package com.dremio.dac.explore.udfs;

import javax.inject.Inject;

import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.dac.explore.udfs.DremioUDFUtils.ExampleUDFOutputDerivation;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;

/**
 * Functions to extract a substring
 */
public class ExtractPosition {
  public static final String GEN_EXAMPLE = "extract_position_example";

  /**
   * Generates a position (start and offset) of substring that is matching the given positions in input.
   * Used for generating card examples.
   *
   * Parameters:
   * 1. in : input column
   * 2. start : 1-based start index. If negative, count from the end.
   * 3. length : length of the string.
   */
  @FunctionTemplate(name = GEN_EXAMPLE, scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL, derivation = ExampleUDFOutputDerivation.class)
  public static class ExtractPositionExample implements SimpleFunction {

    @Param private NullableVarCharHolder in;
    @Param private NullableIntHolder start;
    @Param private NullableIntHolder length;

    @Output private ComplexWriter out;
    @Inject private FunctionErrorContext errCtx;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (in.isSet == 0 || length.isSet == 0) {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
        return;
      }
      final int strLength = com.dremio.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer),
        in.start, in.end, errCtx);
      final int s;
      if (start.value < 0) {
        s = strLength - (-start.value);
      } else if (start.value == 0) {
        s = 0;
      } else {
        s = start.value - 1;
      }

      final int e = java.lang.Math.min(strLength, s + length.value);
      if (s >= 0 && e >= s) {
        com.dremio.dac.explore.udfs.DremioUDFUtils.writeCardExample(out,
            new com.dremio.dac.proto.model.dataset.CardExamplePosition(s, e - s));
      } else {
        out.rootAsList(); // calling this sets the ComplexWriter as a list type.
      }
    }
  }
}
