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
package com.dremio.service.jobs;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

/** Function that waits for a latch. */
public class WaitFunction {

  /** Wait Function */
  @FunctionTemplate(
      name = "wait",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      isDeterministic = false,
      nulls = NullHandling.INTERNAL)
  public static class Wait implements SimpleFunction {

    @Param private VarCharHolder key;
    @Param private IntHolder timeout;
    @Output private NullableBigIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      final String stringKey =
          com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
              key.start, key.end, key.buffer);
      com.dremio.service.jobs.TestJobService.TestingFunctionHelper.tryRun(
          stringKey, timeout.value, java.util.concurrent.TimeUnit.SECONDS);
      out.value = 0;
    }
  }
}
