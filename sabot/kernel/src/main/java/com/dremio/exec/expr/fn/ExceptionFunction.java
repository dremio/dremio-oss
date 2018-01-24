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
package com.dremio.exec.expr.fn;

import javax.inject.Inject;

import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

public class ExceptionFunction {

  public static final String EXCEPTION_FUNCTION_NAME = "__throwException";

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExceptionFunction.class);

  @FunctionTemplate(name = EXCEPTION_FUNCTION_NAME, isDeterministic = false)
  public static class ThrowException implements SimpleFunction {

    @Param VarCharHolder message;
    @Output NullableBigIntHolder out;
    @Inject FunctionErrorContext errorContext;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      String msg = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(message.start, message.end, message.buffer);
      com.dremio.exec.expr.fn.ExceptionFunction.throwException(errorContext, msg);
    }
  }

  public static void throwException(FunctionErrorContext errorContext, String message) {
    throw errorContext.error()
        .message(message)
        .build();
  }
}
