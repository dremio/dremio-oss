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

<#list cast.types as type>

<#if type.major == "IntervalVarChar">  <#-- Template to convert from Interval, IntervalYear, IntervalDay to VarChar -->

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/Cast${type.from}To${type.to}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl.gcast;

<#include "/@includes/vv_imports.ftl" />

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ArrowBuf;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.arrow.vector.util.DateUtility;

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}To${type.to} implements SimpleFunction {

  @Param ${type.from}Holder in;
  @Param BigIntHolder len;
  @Inject ArrowBuf buffer;
  @Output ${type.to}Holder out;

  public void setup() {
    buffer.reallocIfNeeded(${type.bufferLength});
  }

  public void eval() {

      int years  = (in.months / org.apache.arrow.vector.util.DateUtility.yearsToMonths);
      int months = (in.months % org.apache.arrow.vector.util.DateUtility.yearsToMonths);

      long millis = in.milliseconds;

      long hours  = millis / (org.apache.arrow.vector.util.DateUtility.hoursToMillis);
      millis     = millis % (org.apache.arrow.vector.util.DateUtility.hoursToMillis);

      long minutes = millis / (org.apache.arrow.vector.util.DateUtility.minutesToMillis);
      millis      = millis % (org.apache.arrow.vector.util.DateUtility.minutesToMillis);

      long seconds = millis / (org.apache.arrow.vector.util.DateUtility.secondsToMillis);
      millis      = millis % (org.apache.arrow.vector.util.DateUtility.secondsToMillis);

      String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      String monthString = (Math.abs(months) == 1) ? " month " : " months ";
      String dayString = (Math.abs(in.days) == 1) ? " day " : " days ";


      StringBuilder str = new StringBuilder().
                            append(years).append(yearString).
                            append(months).append(monthString).
                            append(in.days).append(dayString).
                            append(hours).append(":").
                            append(minutes).append(":").
                            append(seconds).append(".").
                            append(millis);
      out.buffer = buffer;
      out.start = 0;
      out.end = Math.min((int)len.value, str.length()); // truncate if target type has length smaller than that of input's string
      out.buffer.setBytes(0, String.valueOf(str.substring(0,out.end)).getBytes());
  }
}

<#elseif type.major == "IntervalYearVarChar">
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/Cast${type.from}To${type.to}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl.gcast;

<#include "/@includes/vv_imports.ftl" />

import io.netty.buffer.ByteBuf;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.arrow.vector.util.DateUtility;

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}To${type.to} implements SimpleFunction {

  @Param ${type.from}Holder in;
  @Param BigIntHolder len;
  @Inject ArrowBuf buffer;
  @Output ${type.to}Holder out;

  public void setup() {
    buffer = buffer.reallocIfNeeded((int) len.value);
  }

  public void eval() {
      int years  = (in.value / org.apache.arrow.vector.util.DateUtility.yearsToMonths);
      int months = (in.value % org.apache.arrow.vector.util.DateUtility.yearsToMonths);

      String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      String monthString = (Math.abs(months) == 1) ? " month " : " months ";


      StringBuilder str = new StringBuilder().append(years).append(yearString).append(months).append(monthString);

      out.buffer = buffer;
      out.start = 0;
      out.end = Math.min((int)len.value, str.length()); // truncate if target type has length smaller than that of input's string
      out.buffer.setBytes(0, String.valueOf(str.substring(0,out.end)).getBytes());
  }
}

<#elseif type.major == "IntervalDayVarChar">
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
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL, 
  costCategory = FunctionCostCategory.COMPLEX)
public class Cast${type.from}To${type.to} implements SimpleFunction {

  @Param ${type.from}Holder in;
  @Param BigIntHolder len;
  @Inject ArrowBuf buffer;
  @Output ${type.to}Holder out;

  public void setup() {
    buffer = buffer.reallocIfNeeded((int) len.value);
  }

  public void eval() {
      long millis = in.milliseconds;

      long hours  = millis / (org.apache.arrow.vector.util.DateUtility.hoursToMillis);
      millis     = millis % (org.apache.arrow.vector.util.DateUtility.hoursToMillis);

      long minutes = millis / (org.apache.arrow.vector.util.DateUtility.minutesToMillis);
      millis      = millis % (org.apache.arrow.vector.util.DateUtility.minutesToMillis);

      long seconds = millis / (org.apache.arrow.vector.util.DateUtility.secondsToMillis);
      millis      = millis % (org.apache.arrow.vector.util.DateUtility.secondsToMillis);

      String dayString = (Math.abs(in.days) == 1) ? " day " : " days ";


      StringBuilder str = new StringBuilder().
                            append(in.days).append(dayString).
                            append(hours).append(":").
                            append(minutes).append(":").
                            append(seconds).append(".").
                            append(millis);


      out.buffer = buffer;
      out.start = 0;
      out.end = Math.min((int)len.value, str.length()); // truncate if target type has length smaller than that of input's string
      out.buffer.setBytes(0, String.valueOf(str.substring(0,out.end)).getBytes());
  }
}
</#if> <#-- type.major -->
</#list>
