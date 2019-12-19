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
<#if type.major == "Date">  <#-- Template file to convert between Date types (Date, TimeStamp) -->

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/Cast${type.from}To${type.to}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl.gcast;

import io.netty.buffer.ByteBuf;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.vector.holders.*;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.arrow.vector.util.DateUtility;
/**
 * generated from ${.template_name} ${type.from} ${type.to} ${type.major}
 */
<#assign typeMapping = TypeMappings[type.to]!{}>
<#assign dremioMinorType = typeMapping.minor_type!type.to?upper_case>
@SuppressWarnings("unused")
@FunctionTemplate(
        <#if type.to == "DateMilli">
        names = {"cast${dremioMinorType}", "${type.alias}"}
        <#elseif type.to == "TimeStampMilli">
        name = "castTIMESTAMP"
        <#else>
          <#stop "Unsupported type ${type.to}">
        </#if>
        , scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL, costCategory=FunctionCostCategory.COMPLEX)
public class Cast${type.from}To${type.to} implements SimpleFunction {

  @Param ${type.from}Holder in;
  @Output ${type.to}Holder out;

  public void setup() {
  }

  public void eval() {
    <#if type.to == "DateMilli">
    out.value = (new org.joda.time.DateMidnight(in.value, org.joda.time.DateTimeZone.UTC)).getMillis();
    <#elseif type.to == "TimeStampMilli">
    out.value = in.value;
    </#if>
  }
}
</#if> <#-- type.major -->
</#list>
