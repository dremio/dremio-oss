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


<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GNewPartitionNumberFunctions.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;
import javax.inject.Inject;
import io.netty.buffer.ArrowBuf;

/**
 * generated from ${.template_name} 
 */
public class GNewPartitionNumberFunctions {
<#list vv.types as type>
<#if type.major == "Fixed" || type.major = "Bit">

<#list type.minor as minor>
<#assign typeMapping = TypeMappings[minor.class]!{}>
<#assign supported = typeMapping.supported!true>
<#if supported>
<#list vv.modes as mode>

  <#if mode.name != "Repeated" && mode.name != "Required" && !minor.class?starts_with("UInt") && !minor.class?starts_with("SmallInt") && !minor.class?starts_with("TinyInt") >

<#if !minor.class.startsWith("Decimal") && !minor.class.startsWith("Interval") && (minor.class != "FixedSizeBinary")>
@SuppressWarnings("unused")
@FunctionTemplate(name = "newPartitionNumber", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
public static class NewPartitionNumber${minor.class}${mode.prefix} implements SimpleFunction{

  @Param ${mode.prefix}${minor.class}Holder in;
  @Workspace ${mode.prefix}${minor.class}Holder previous;
  @Workspace int partitionNumber;
  @Output NullableIntHolder out;

  public void setup() {
    partitionNumber = -1;
  }

  <#if mode.name == "Required">
  public void eval() {
    out.isSet = 1;
    if (partitionNumber != -1) {
      if (in.value == previous.value) {
        out.value = partitionNumber;
      } else {
        previous.value = in.value;
        partitionNumber = partitionNumber + 1;
        out.value = partitionNumber;
      }
    } else {
      previous.value = in.value;
      out.value = 1;
      partitionNumber = 0;
    }
  }
  </#if>

  <#if (mode.name == "Optional") && (minor.class != "FixedSizeBinary")>
  public void eval() {
    if (partitionNumber != -1) {
      if (in.isSet == 0 && previous.isSet == 0) {
        out.value = partitionNumber;
      } else if (in.value == previous.value) {
        out.value = partitionNumber;
      } else {
        previous.value = in.value;
        previous.isSet = in.isSet;
        partitionNumber = partitionNumber + 1;
        out.value = partitionNumber;
      }
    } else {
      previous.value = in.value;
      previous.isSet = in.isSet;
      out.value = 1;
      partitionNumber = 0;
    }
    
    out.isSet = 1;
  }
  </#if>
}
</#if> <#-- minor.class.startWith -->

</#if> <#-- mode.name -->
</#list>
</#if> <#-- supported -->
</#list>
</#if> <#-- type.major -->
</#list>
}
