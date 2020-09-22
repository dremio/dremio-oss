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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

/**
 * UDFs for ConvertCase
 */
public class ConvertCase {

  /**
   * Convert string to title case.
   */
  @FunctionTemplate(name = "title", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class TitleCase implements SimpleFunction {
    @Param
    private VarCharHolder input;
    @Output
    private VarCharHolder out;
    @Inject
    private ArrowBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer.reallocIfNeeded(input.end - input.start);
      buffer = out.buffer;
      out.start = 0;
      out.end = input.end - input.start;

      for (int id = input.start; id < input.end; id++) {
        byte currentByte = input.buffer.getByte(id);

        // 'A - Z' : 0x41 - 0x5A
        // 'a - z' : 0x61 - 0x7A
        // space : 0x20
        if (currentByte >= 0x61 && currentByte <= 0x7A
                && (id == input.start || input.buffer.getByte(id - 1) == 0x20)) {
          currentByte -= 0x20;
        }
        if (currentByte >= 0x41 && currentByte <= 0x5A
                && !(id == input.start || input.buffer.getByte(id - 1) == 0x20)) {
          currentByte += 0x20;
        }
        out.buffer.setByte(id - input.start, currentByte);
      }
    }
  }
}
