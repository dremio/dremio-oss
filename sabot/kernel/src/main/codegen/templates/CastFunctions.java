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
<@pp.dropOutputFile />



<#list cast.types as type>
<#if type.major == "Fixed">

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/Cast${type.from}${type.to}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl.gcast;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.vector.holders.*;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;

/**
 * generated from ${.template_name} ${type.from} ${type.to} ${type.major}
 */
@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements SimpleFunction{

  @Param ${type.from}Holder in;
  @Output ${type.to}Holder out;

  public void setup() {}

  public void eval() {
    <#if (type.from.startsWith("Float") && type.to.endsWith("Int"))>
    boolean sign = (in.value < 0);
    in.value = java.lang.Math.abs(in.value);
    ${type.native} fractional = in.value % 1;
    int digit = ((int) (fractional * 10));
    int carry = 0;
    if (digit > 4) {
      carry = 1;
    }
    out.value = ((${type.explicit}) in.value) + carry;
    if (sign == true) {
      out.value *= -1;
    }
    <#elseif type.explicit??>
    out.value = (${type.explicit}) in.value;
    <#else>
    out.value = in.value;
    </#if>
  }
}

</#if> <#-- type.major -->
</#list>

