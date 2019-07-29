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


<#list intervalNumericTypes.interval as intervaltype>

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/${intervaltype}Functions.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

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

/**
 * generated from ${.template_name}
 */
public class ${intervaltype}Functions {

    @SuppressWarnings("unused")
    @FunctionTemplate(name = "add", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}AddFunction implements SimpleFunction {
    @Param ${intervaltype}Holder left;
    @Param ${intervaltype}Holder right;
    @Output ${intervaltype}Holder out;

        public void setup() {
        }

        public void eval() {
            <#if intervaltype == "Interval">
            out.months = left.months + right.months;
            out.days = left.days + right.days;
            out.milliseconds = left.milliseconds + right.milliseconds;
            <#elseif intervaltype == "IntervalYear">
            out.value = left.value + right.value;
            <#elseif intervaltype == "IntervalDay">
            out.days = left.days + right.days;
            out.milliseconds = left.milliseconds + right.milliseconds;
            </#if>
        }
    }

    @SuppressWarnings("unused")
    @FunctionTemplate(name = "subtract", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}SubtractFunction implements SimpleFunction {
    @Param ${intervaltype}Holder left;
    @Param ${intervaltype}Holder right;
    @Output ${intervaltype}Holder out;

        public void setup() {
        }

        public void eval() {
            <#if intervaltype == "Interval">
            out.months = left.months - right.months;
            out.days = left.days - right.days;
            out.milliseconds = left.milliseconds - right.milliseconds;
            <#elseif intervaltype == "IntervalYear">
            out.value = left.value - right.value;
            <#elseif intervaltype == "IntervalDay">
            out.days = left.days - right.days;
            out.milliseconds = left.milliseconds - right.milliseconds;
            </#if>
        }
    }
    @SuppressWarnings("unused")
    @FunctionTemplate(names = {"negative", "u-", "-"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
    public static class ${intervaltype}NegateFunction implements SimpleFunction {
    @Param ${intervaltype}Holder left;
    @Output ${intervaltype}Holder out;

        public void setup() {
        }

        public void eval() {
            <#if intervaltype == "Interval">
            out.months = -left.months;
            out.days = -left.days;
            out.milliseconds = -left.milliseconds;
            <#elseif intervaltype == "IntervalYear">
            out.value = -left.value;
            <#elseif intervaltype == "IntervalDay">
            out.days = -left.days;
            out.milliseconds = -left.milliseconds;
            </#if>
        }
    }
}
</#list>
