/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.op.llvm;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;

public abstract class NativeProjectEvaluator implements AutoCloseable {

  public abstract void evaluate(int recordCount) throws Exception;

  public static NativeProjectorBuilder builder(VectorAccessible input, FunctionContext functionContext) {
    return new NativeProjectorBuilder(input, functionContext);
  }


}
