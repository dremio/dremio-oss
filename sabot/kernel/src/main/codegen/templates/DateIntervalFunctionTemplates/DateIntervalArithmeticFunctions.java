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


<#list dateIntervalFunc.dates as datetype>
<#list dateIntervalFunc.intervals as intervaltype>

<#if datetype == "DateMilli" || datetype == "TimeStampMilli">
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/${datetype}${intervaltype}Functions.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import io.netty.buffer.ByteBuf;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import org.apache.arrow.vector.holders.*;
import org.joda.time.MutableDateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.arrow.vector.util.DateUtility;
import javax.inject.Inject;

/**
 * generated from ${.template_name}
 */
public class ${datetype}${intervaltype}Functions {

<#macro dateIntervalArithmeticBlock left right temp op output intervaltype datetype errctx>

    <#-- Throw exception if we are adding integer to a TIMESTAMP -->
    <#if (datetype == "TimeStampMilli") && (intervaltype == "Int" || intervaltype == "BigInt")>
    if (1 == 1) {
        /* Since this will be included in the run time generated code, there might be other logic that follows this
         * if the exception is raised without a condition, we will hit compilation issues while compiling run time code
         * with the error: unreachable code.
         */
        throw ${errctx}.error()
          .message("Cannot add integer to TIMESTAMP, cast it to specific interval")
          .build();
    }
    <#else>
    ${temp}.setMillis(${left}.value);

    <#if intervaltype == "Interval">
    ${temp}.addMonths(${right}.months <#if op == '-'> * -1 </#if>);
    ${temp}.addDays(${right}.days <#if op == '-'> * -1 </#if>);
    ${temp}.add(${right}.milliseconds <#if op == '-'> * -1 </#if>);
    <#elseif intervaltype == "IntervalYear">
    ${temp}.addMonths(${right}.value <#if op == '-'> * -1 </#if>);
    <#elseif intervaltype == "IntervalDay">
    ${temp}.addDays(${right}.days <#if op == '-'> * -1 </#if>);
    ${temp}.add(${right}.milliseconds <#if op == '-'> * -1 </#if>);
    <#elseif intervaltype == "Int" || intervaltype == "BigInt">
    ${temp}.addDays((int) ${right}.value <#if op == '-'> * -1 </#if>);
    </#if>

    ${output}.value = ${temp}.getMillis();
    </#if>
</#macro>

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${datetype}${intervaltype}AddFunction implements SimpleFunction {
    @Param ${datetype}Holder left;
    @Param ${intervaltype}Holder right;
    @Workspace org.joda.time.MutableDateTime temp;
    <#if datetype == "DateMilli" && (intervaltype.startsWith("Interval"))>
    @Output TimeStampMilliHolder out;
    <#else>
    @Output ${datetype}Holder out;
    </#if>
    @Inject FunctionErrorContext errCtx;
        public void setup() {
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
        }

        public void eval() {
            <@dateIntervalArithmeticBlock left="left" right="right" temp="temp" op="+" output="out" intervaltype=intervaltype datetype=datetype errctx="errCtx" />
        }
    }

    <#-- Below function is the same as above except the arguments are in reverse order. We use macros to avoid having the logic in multiple places -->
    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}${datetype}AddFunction implements SimpleFunction {

    @Param ${intervaltype}Holder right;
    @Param ${datetype}Holder left;
    @Workspace org.joda.time.MutableDateTime temp;
    <#if datetype == "DateMilli" && (intervaltype.startsWith("Interval"))>
    @Output TimeStampMilliHolder out;
    <#else>
    @Output ${datetype}Holder out;
    </#if>
    @Inject FunctionErrorContext errCtx;
        public void setup() {
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
        }

        public void eval() {

            <@dateIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "+" output="out" intervaltype=intervaltype datetype = datetype errctx="errCtx"/>
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_sub", "subtract", "date_diff"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${datetype}${intervaltype}SubtractFunction implements SimpleFunction {
    @Param ${datetype}Holder left;
    @Param ${intervaltype}Holder right;
    @Workspace org.joda.time.MutableDateTime temp;
    <#if datetype == "DateMilli" && (intervaltype.startsWith("Interval"))>
    @Output TimeStampMilliHolder out;
    <#else>
    @Output ${datetype}Holder out;
    </#if>
    @Inject FunctionErrorContext errCtx;

        public void setup() {
            temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
        }

        public void eval() {
            <@dateIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "-" output="out" intervaltype=intervaltype datetype = datetype errctx="errCtx"/>
        }
    }
}
<#elseif datetype == "TimeMilli">
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/${datetype}${intervaltype}Functions.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import io.netty.buffer.ByteBuf;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import org.apache.arrow.vector.holders.*;
import org.joda.time.MutableDateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.arrow.vector.util.DateUtility;
import javax.inject.Inject;

/**
 * generated from ${.template_name} ${datetype} ${intervaltype}
 */
public class ${datetype}${intervaltype}Functions {
<#macro timeIntervalArithmeticBlock left right temp op output intervaltype errctx>
    <#if intervaltype == "Int" || intervaltype == "BigInt">
    if (1 == 1) {
        throw ${errctx}.error()
          .message("Cannot add integer to TIME, cast it to specific interval")
          .build();
    }
    <#elseif intervaltype == "IntervalYear">
    // Needn't add anything to time from interval year data type. Output is same as input
    ${output} = ${left}.value;
    <#else>
    ${output} = ${left}.value ${op} ${right}.milliseconds;
    // Wrap around 24 hour clock if we exceeded it while adding the time component
    ${output} = ${output} % org.apache.arrow.vector.util.DateUtility.daysToStandardMillis;
    </#if>
</#macro>

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${datetype}${intervaltype}AddFunction implements SimpleFunction {
    @Param ${datetype}Holder left;
    @Param ${intervaltype}Holder right;
    @Output ${datetype}Holder out;
    @Inject FunctionErrorContext errCtx;

        public void setup() {
        }

        public void eval() {
            <@timeIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "+" output="out.value" intervaltype=intervaltype errctx="errCtx"/>
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_add", "add"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}${datetype}AddFunction implements SimpleFunction {
    @Param ${intervaltype}Holder right;
    @Param ${datetype}Holder left;
    @Output ${datetype}Holder out;
    @Inject FunctionErrorContext errCtx;

        public void setup() {
        }
        public void eval() {
            <@timeIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "+" output="out.value" intervaltype=intervaltype errctx="errCtx" />
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"date_sub", "subtract", "date_diff"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${datetype}${intervaltype}SubtractFunction implements SimpleFunction {
    @Param ${datetype}Holder left;
    @Param ${intervaltype}Holder right;
    @Output ${datetype}Holder out;
    @Inject FunctionErrorContext errCtx;

        public void setup() {
        }

        public void eval() {
            <@timeIntervalArithmeticBlock left="left" right="right" temp = "temp" op = "-" output="out.value" intervaltype=intervaltype errctx="errCtx" />
        }
    }
}
</#if>
</#list>
</#list>
