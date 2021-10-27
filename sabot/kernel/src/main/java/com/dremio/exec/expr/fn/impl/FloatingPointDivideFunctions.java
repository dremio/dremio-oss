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

import javax.inject.Inject;

import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;

@SuppressWarnings("unused")

/**
 * generated from MathFunctionTemplates.java
 */
public class FloatingPointDivideFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DivideFunctions.class);


  @FunctionTemplate(name = "divide", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Float4Float4Divide implements SimpleFunction {

    @Param Float4Holder in1;
    @Param Float4Holder in2;
    @Output Float4Holder out;
    @Inject FunctionErrorContext errCtx;


    public void setup() {
    }

    public void eval() {
      if(in2.value == 0.0) {
        throw errCtx.error()
          .message("divide by zero")
          .build();
      } else {
        out.value = in1.value / in2.value;
      }
    }
  }

  @FunctionTemplate(name = "divide", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Float8Float8Divide implements SimpleFunction {

    @Param Float8Holder in1;
    @Param Float8Holder in2;
    @Output Float8Holder out;
    @Inject FunctionErrorContext errCtx;


    public void setup() {
    }

    public void eval() {
      if(in2.value == 0.0) {
        throw errCtx.error()
          .message("divide by zero")
          .build();
      } else {
        out.value = in1.value / in2.value;
      }
    }
  }
}
