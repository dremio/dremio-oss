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
<@pp.dropOutputFile />



<#list logicalTypes.logicalAggrTypes as aggrtype>
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gaggr/${aggrtype.className}Functions.java" />

<#include "/@includes/license.ftl" />

<#-- A utility class that is used to generate java code for aggr functions bit_and / bit_or -->

/*
 * This class is automatically generated from AggrBitwiseLogicalTypes.tdd using FreeMarker.
 */

package com.dremio.exec.expr.fn.impl.gaggr;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;


@SuppressWarnings("unused")

public class ${aggrtype.className}Functions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>

<#if aggrtype.aliasName == "">
@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
<#else>
@FunctionTemplate(names = {"${aggrtype.funcName}", "${aggrtype.aliasName}"}, scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
</#if>
public static class ${type.inputType}${aggrtype.className} implements AggrFunction{

  @Param ${type.inputType}Holder in;
  @Workspace ${type.outputType}Holder inter;
  @Workspace BigIntHolder nonNullCount;
  @Output ${type.outputType}Holder out;

  public void setup() {
    inter = new ${type.outputType}Holder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;

    // Initialize the workspace variables
  <#if aggrtype.funcName == "bit_and">
    inter.value = ${type.maxval}.MAX_VALUE;
    <#elseif aggrtype.funcName == "bit_or">
    inter.value = 0;
  </#if>
  }

  @Override
  public void add() {
  <#if type.inputType?starts_with("Nullable")>
    sout: {
    if (in.isSet == 0) {
     // processing nullable input and the value is null, so don't do anything...
     break sout;
    }
  </#if>
    nonNullCount.value = 1;
  <#if aggrtype.funcName == "bit_and">
    inter.value = <#if type.extraCast ??>(${type.extraCast})</#if>(inter.value & in.value);
    <#elseif aggrtype.funcName == "bit_or">
    inter.value = <#if type.extraCast ??>(${type.extraCast})</#if>(inter.value | in.value);
  </#if>

    <#if type.inputType?starts_with("Nullable")>
    } // end of sout block
    </#if>
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0) {
      out.isSet = 1;
      <#if aggrtype.funcName == "bit_and">
        out.value = inter.value;
        <#elseif aggrtype.funcName == "bit_or">
        out.value = inter.value;
      </#if>
    } else {
      out.isSet = 0;
    }
  }

  @Override
  public void reset() {
    nonNullCount.value = 0;
    <#if aggrtype.funcName == "bit_and">
      inter.value = ${type.maxval}.MAX_VALUE;
      <#elseif aggrtype.funcName == "bit_or">
      inter.value = 0;
    </#if>
  }
}


</#list>
}
</#list>
