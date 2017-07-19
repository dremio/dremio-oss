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

<#list cast.types as type>
<#if type.major == "VarCharInterval">  <#-- Template to convert from VarChar to Interval, IntervalYear, IntervalDay -->

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/Cast${type.from}To${type.to}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl.gcast;

import io.netty.buffer.ByteBuf;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.arrow.vector.util.DateUtility;
import javax.inject.Inject;
import io.netty.buffer.ArrowBuf;

/**
 * generated from ${.template_name} ${type.from} ${type.to} ${type.major}
 */
@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}To${type.to} implements SimpleFunction {

  @Param ${type.from}Holder in;
  @Output ${type.to}Holder out;

  public void setup() {
  }

  public void eval() {

      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      String input = new String(buf, com.google.common.base.Charsets.UTF_8);

      // Parse the ISO format
      org.joda.time.Period period = org.joda.time.Period.parse(input);

      <#if type.to == "Interval">
      out.months       = (period.getYears() * org.apache.arrow.vector.util.DateUtility.yearsToMonths) + period.getMonths();

      out.days         = period.getDays();

      out.milliseconds = (period.getHours() * org.apache.arrow.vector.util.DateUtility.hoursToMillis) +
                         (period.getMinutes() * org.apache.arrow.vector.util.DateUtility.minutesToMillis) +
                         (period.getSeconds() * org.apache.arrow.vector.util.DateUtility.secondsToMillis) +
                         (period.getMillis());

      <#elseif type.to == "IntervalDay">
      out.days         = period.getDays();

      out.milliseconds = (period.getHours() * org.apache.arrow.vector.util.DateUtility.hoursToMillis) +
                         (period.getMinutes() * org.apache.arrow.vector.util.DateUtility.minutesToMillis) +
                         (period.getSeconds() * org.apache.arrow.vector.util.DateUtility.secondsToMillis) +
                         (period.getMillis());
      <#elseif type.to == "IntervalYear">
      out.value = (period.getYears() * org.apache.arrow.vector.util.DateUtility.yearsToMonths) + period.getMonths();
      </#if>
  }
}
</#if> <#-- type.major -->
</#list>
