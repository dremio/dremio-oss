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
<#list vv.types as type>
<#list type.minor as minor>
<#assign typeMapping = TypeMappings[minor.class]!{}>
<#assign supported = typeMapping.supported!true>
<#if supported>
<#assign className="GNullOpNullable${minor.class}Holder" />

<#if !minor.class?starts_with("UInt") && !minor.class?starts_with("SmallInt") && !minor.class?starts_with("TinyInt") && !minor.class?starts_with("IntervalMonthDayNano") >
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/${className}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.*;
import org.apache.arrow.vector.holders.*;

/**
 * generated from ${.template_name}
 */
public class ${className} {

  @FunctionTemplate(names = {"isnull", "is null"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IsNull implements SimpleFunction {

    @Param Nullable${minor.class}Holder input;
    @Output NullableBitHolder out;

    public void setup() { }

    public void eval() {
      out.isSet = 1;
      out.value = (input.isSet == 0 ? 1 : 0);
    }
  }

  @FunctionTemplate(names = {"isnotnull", "is not null"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IsNotNull implements SimpleFunction {

    @Param Nullable${minor.class}Holder input;
    @Output NullableBitHolder out;

    public void setup() { }

    public void eval() {
      out.isSet = 1;
      out.value = (input.isSet == 0 ? 0 : 1);
    }
  }
}
</#if>
</#if>
</#list>
</#list>
