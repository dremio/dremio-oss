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
<@pp.dropOutputFile />



<#list MathFunctionTypes.mathFunctionTypes as inputType>
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/${inputType.className}Functions.java" />

<#include "/@includes/license.ftl" />

<#-- A utility class that is used to generate java code for add function -->

/*
 * This class is automatically generated from AddTypes.tdd using FreeMarker.
 */


package com.dremio.exec.expr.fn.impl;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;

@SuppressWarnings("unused")

/**
 * generated from ${.template_name} 
 */
public class ${inputType.className}Functions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${inputType.className}Functions.class);

<#list inputType.types as type>

  @FunctionTemplate(name = "${inputType.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${type.input1}${type.input2}${inputType.className} implements SimpleFunction {

    @Param ${type.input1}Holder in1;
    @Param ${type.input2}Holder in2;
    @Output ${type.outputType}Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = (${type.castType}) (in1.value ${inputType.op} in2.value);
    }
  }
</#list>
}
</#list>
