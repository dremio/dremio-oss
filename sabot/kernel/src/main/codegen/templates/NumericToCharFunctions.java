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

import com.dremio.exec.expr.annotations.Workspace;

<@pp.dropOutputFile />

<#list numericTypes.numeric as type>

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/G${type}ToChar.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

<#include "/@includes/vv_imports.ftl" />

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.vector.holders.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ArrowBuf;

import java.text.NumberFormat;
import java.text.DecimalFormat;

@SuppressWarnings("unused")
@FunctionTemplate(name = "to_char", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class G${type}ToChar implements SimpleFunction {

    @Param  ${type}Holder left;
    @Param  VarCharHolder right;
    @Inject ArrowBuf buffer;
    @Workspace java.text.NumberFormat outputFormat;
    @Output VarCharHolder out;

    public void setup() {
        buffer = buffer.reallocIfNeeded(100);
        byte[] buf = new byte[right.end - right.start];
        right.buffer.getBytes(right.start, buf, 0, right.end - right.start);
        String inputFormat = new String(buf);
        outputFormat = new java.text.DecimalFormat(inputFormat);
    }

    public void eval() {

        <#if type == "Decimal9" || type == "Decimal18">
        java.math.BigDecimal bigDecimal = new java.math.BigDecimal(java.math.BigInteger.valueOf(left.value), left.scale);
        String str = outputFormat.format(bigDecimal);
        <#elseif type == "Decimal28Sparse" || type == "Decimal38Sparse">
        java.math.BigDecimal bigDecimal = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(left.buffer, left.start, left.nDecimalDigits, left.scale, true);
        String str = outputFormat.format(bigDecimal);
        <#elseif type == "Decimal28Dense" || type == "Decimal38Dense">
        java.math.BigDecimal bigDecimal = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromDense(left.buffer, left.start, left.nDecimalDigits, left.scale, left.maxPrecision, left.WIDTH);
        String str = outputFormat.format(bigDecimal);
        <#else>
        String str =  outputFormat.format(left.value);
        </#if>
        out.buffer = buffer;
        out.start = 0;
        out.end = Math.min(100, str.length()); // truncate if target type has length smaller than that of input's string
        out.buffer.setBytes(0, str.substring(0,out.end).getBytes());

    }
}
</#list>
