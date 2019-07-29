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



<#list aggrtypes1.aggrtypes as aggrtype>
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gaggr/${aggrtype.className}DateTypeFunctions.java" />

<#include "/@includes/license.ftl" />

// Source code generated using FreeMarker template ${.template_name}

<#-- A utility class that is used to generate java code for aggr functions for Date, Time, Interval types -->
<#--  that maintain a single running counter to hold the result.  This includes: MIN, MAX, SUM, COUNT. -->

package com.dremio.exec.expr.fn.impl.gaggr;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;

@SuppressWarnings("unused")

/**
 * generated from ${.template_name} ${aggrtype.className}
 */
public class ${aggrtype.className}DateTypeFunctions {

<#list aggrtype.types as type>
<#if type.major == "Date">
@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class ${type.inputType}${aggrtype.className} implements AggrFunction{

  @Param ${type.inputType}Holder in;
  @Workspace ${type.runningType}Holder value;
  @Workspace NullableBigIntHolder nonNullCount;
  @Output ${type.outputType}Holder out;

  public void setup() {
	  value = new ${type.runningType}Holder();
    nonNullCount = new NullableBigIntHolder();
    nonNullCount.value = 0;
    <#if type.runningType == "NullableInterval">
    value.months = ${type.initialValue};
    value.days= ${type.initialValue};
    value.milliseconds = ${type.initialValue};
    <#elseif type.runningType == "NullableIntervalDay">
    value.days= ${type.initialValue};
    value.milliseconds = ${type.initialValue};
    <#else>
    value.value = ${type.initialValue};
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
	  <#if aggrtype.funcName == "min">

    <#if type.outputType?ends_with("Interval")>

    long inMS = (long) in.months * org.apache.arrow.vector.util.DateUtility.monthsToMillis+
                       in.days * (org.apache.arrow.vector.util.DateUtility.daysToStandardMillis) +
                       in.milliseconds;

    value.value = Math.min(value.value, inMS);

    <#elseif type.outputType?ends_with("IntervalDay")>
    long inMS = (long) in.days * (org.apache.arrow.vector.util.DateUtility.daysToStandardMillis) +
                       in.milliseconds;

    value.value = Math.min(value.value, inMS);


    <#else>
    value.value = Math.min(value.value, in.value);
    </#if>
	  <#elseif aggrtype.funcName == "max">
    <#if type.outputType?ends_with("Interval")>
    long inMS = (long) in.months * org.apache.arrow.vector.util.DateUtility.monthsToMillis+
                       in.days * (org.apache.arrow.vector.util.DateUtility.daysToStandardMillis) +
                       in.milliseconds;

    value.value = Math.max(value.value, inMS);
    <#elseif type.outputType?ends_with("IntervalDay")>
    long inMS = (long) in.days * (org.apache.arrow.vector.util.DateUtility.daysToStandardMillis) +
                       in.milliseconds;

    value.value = Math.max(value.value, inMS);
    <#else>
    value.value = Math.max(value.value, in.value);
    </#if>

	  <#elseif aggrtype.funcName == "sum">
    <#if type.outputType?ends_with("Interval")>
    value.days += in.days;
    value.months += in.months;
    value.milliseconds += in.milliseconds;
    <#elseif type.outputType?ends_with("IntervalDay")>
    value.days += in.days;
    value.milliseconds += in.milliseconds;
    <#else>
	    value.value += in.value;
    </#if>
	  <#elseif aggrtype.funcName == "count">
	    value.value++;
	  <#else>
	  // TODO: throw an error ?
	  </#if>
	<#if type.inputType?starts_with("Nullable")>
    } // end of sout block
	</#if>
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0) {
      out.isSet = 1;
      <#if aggrtype.funcName == "max" || aggrtype.funcName == "min">
      <#if type.outputType?ends_with("Interval")>
      out.months = (int) (value.value / org.apache.arrow.vector.util.DateUtility.monthsToMillis);
      value.value = value.value % org.apache.arrow.vector.util.DateUtility.monthsToMillis;
      out.days = (int) (value.value / org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
      out.milliseconds = (int) (value.value % org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
      <#elseif type.outputType?ends_with("IntervalDay")>
      out.days = (int) (value.value / org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
      out.milliseconds = (int) (value.value % org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
      <#else>
      out.value = value.value;
      </#if>
      <#else>
      <#if type.outputType?ends_with("Interval")>
      out.months = value.months;
      out.days = value.days;
      out.milliseconds = value.milliseconds;
      <#elseif type.outputType?ends_with("IntervalDay")>
      out.days = value.days;
      out.milliseconds = value.milliseconds;
      <#else>
      out.value = value.value;
      </#if>
      </#if>
    } else {
      out.isSet = 0;
    }
  }

  @Override
  public void reset() {
    nonNullCount.value = 0;
    <#if type.runningType == "NullableInterval">
    value.months = ${type.initialValue};
    value.days= ${type.initialValue};
    value.milliseconds = ${type.initialValue};
    <#elseif type.runningType == "NullableIntervalDay">
    value.days= ${type.initialValue};
    value.milliseconds = ${type.initialValue};
    <#else>
    value.value = ${type.initialValue};
    </#if>
  }

 }

</#if>
</#list>
}
</#list>

