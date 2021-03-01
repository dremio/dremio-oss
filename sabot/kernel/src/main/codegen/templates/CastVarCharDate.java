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
<#if type.major == "VarCharDate" || type.major == "VarBinaryDate">  <#-- Template to convert from VarChar/ VarBinary to Date, Time, TimeStamp -->

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
import com.dremio.exec.expr.fn.FunctionErrorContext;
import org.apache.arrow.vector.holders.*;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import com.dremio.common.util.JodaDateUtility;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;

/**
 * generated from ${.template_name} ${type.from} ${type.to} ${type.major}
 */
<#assign typeMapping = TypeMappings[type.to]!{}>
<#assign dremioMinorType = typeMapping.minor_type!type.to?upper_case>
@SuppressWarnings("unused")
@FunctionTemplate(names = {"cast${dremioMinorType}", "${type.alias}"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL, 
  costCategory = FunctionCostCategory.COMPLEX)
public class Cast${type.from}To${type.to} implements SimpleFunction {

  @Param ${type.from}Holder in;
  @Output ${type.to}Holder out;
  @Inject FunctionErrorContext errCtx;

  public void setup() {
  }

  public void eval() {

      <#if type.to != "DateMilli">
      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      String input = new String(buf, com.google.common.base.Charsets.UTF_8);
      </#if>  

      try {
        <#if type.to == "DateMilli">
        out.value = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.getDate(in.buffer, in.start, in.end);

        <#elseif type.to == "TimeStampMilli">
        org.joda.time.format.DateTimeFormatter f = com.dremio.common.util.JodaDateUtility.getDateTimeFormatter();
        out.value = com.dremio.common.util.DateTimes.toMillis(org.joda.time.LocalDateTime.parse(input, f));

        <#elseif type.to == "TimeMilli">
        org.joda.time.format.DateTimeFormatter f = com.dremio.common.util.JodaDateUtility.getTimeFormatter();
        if (input.length() == 5) {
          input += ":00";
        }
        out.value = (int) com.dremio.common.util.DateTimes.toMillis((f.parseLocalDateTime(input)));
        </#if>
      } catch (RuntimeException e) {
        throw errCtx.error(e)
          .build();
      }

  }
}
</#if> <#-- type.major -->
</#list>
