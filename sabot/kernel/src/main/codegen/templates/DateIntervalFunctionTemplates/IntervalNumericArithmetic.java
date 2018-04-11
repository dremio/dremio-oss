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

<#list intervalNumericTypes.interval as intervaltype>
<#list intervalNumericTypes.numeric as numerictype>

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/${intervaltype}${numerictype}Functions.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import io.netty.buffer.ByteBuf;

import org.apache.arrow.vector.holders.*;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.arrow.vector.util.DateUtility;

/**
 * generated from ${.template_name}
 */
public class ${intervaltype}${numerictype}Functions {

<#-- Macro block to multiply interval data type with integers -->
<#macro intervalIntegerMultiplyBlock left right temp out intervaltype>
    <#if intervaltype == "IntervalYear">
    ${out}.value = (int) (${left}.value * ${right}.value);
    <#elseif intervaltype == "IntervalDay">
    ${out}.days = (int) (${left}.days * ${right}.value);
    ${out}.milliseconds = (int) (${left}.milliseconds * ${right}.value);
    </#if>
</#macro>

<#-- Macro block to multiply and divide interval data type with integers, floating point values -->
<#macro intervalNumericArithmeticBlock left right temp op out intervaltype>
    <#if intervaltype == "IntervalYear">
    ${out}.value = (int) (${left}.value ${op} (double) ${right}.value);
    <#elseif intervaltype == "IntervalDay">
        long totalMillis = left.days * org.apache.arrow.vector.util.DateUtility.daysToStandardMillis + left.milliseconds;

        totalMillis = (long) (totalMillis ${op} (double) right.value);

        int days = (int) (totalMillis / org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
        int millis = (int) (totalMillis - days * org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
        out.days = days;
        out.milliseconds = millis;
    </#if>
</#macro>

    @SuppressWarnings("unused")
    @FunctionTemplate(name = "multiply", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}${numerictype}MultiplyFunction implements SimpleFunction {
    @Param ${intervaltype}Holder left;
    @Param ${numerictype}Holder right;
    @Output ${intervaltype}Holder out;

        public void setup() {
        }

        public void eval() {
            <@intervalNumericArithmeticBlock left="left" right="right" temp = "temp" op = "*" out = "out" intervaltype=intervaltype />
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(name = "multiply", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${numerictype}${intervaltype}MultiplyFunction implements SimpleFunction {
    @Param ${numerictype}Holder right;
    @Param ${intervaltype}Holder left;
    @Output ${intervaltype}Holder out;

        public void setup() {
        }

        public void eval() {
            <@intervalNumericArithmeticBlock left="left" right="right" temp = "temp" op = "*" out = "out" intervaltype=intervaltype />
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"divide", "div"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}${numerictype}DivideFunction implements SimpleFunction {
    @Param ${intervaltype}Holder left;
    @Param ${numerictype}Holder right;
    @Output ${intervaltype}Holder out;

        public void setup() {
        }

        public void eval() {
            <@intervalNumericArithmeticBlock left="left" right="right" temp = "temp" op = "/" out = "out" intervaltype=intervaltype />
        }
    }
}
</#list>
</#list>
