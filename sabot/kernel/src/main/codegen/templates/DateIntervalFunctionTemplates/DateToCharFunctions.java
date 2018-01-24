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

import com.dremio.exec.expr.annotations.Workspace;

<@pp.dropOutputFile />

<#list dateIntervalFunc.dates as type>

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
import com.dremio.exec.expr.fn.FunctionErrorContext;
import org.apache.arrow.vector.holders.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ArrowBuf;
import javax.inject.Inject;

/**
 * generated from ${.template_name} ${type}
 */
@SuppressWarnings("unused")
@FunctionTemplate(name = "to_char", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class G${type}ToChar implements SimpleFunction {

    @Param  ${type}Holder left;
    @Param  VarCharHolder right;
    @Inject ArrowBuf buffer;
    @Workspace org.joda.time.format.DateTimeFormatter format;
    @Output VarCharHolder out;
    @Inject FunctionErrorContext errCtx;

    public void setup() {
        buffer = buffer.reallocIfNeeded(100);

        // Get the desired output format and create a DateTimeFormatter
        byte[] buf = new byte[right.end - right.start];
        right.buffer.getBytes(right.start, buf, 0, right.end - right.start);
        String input = new String(buf, com.google.common.base.Charsets.UTF_8);
        try {
          format = com.dremio.exec.expr.fn.impl.DateFunctionsUtils.getFormatterForFormatString(input).withZoneUTC();
        } catch (IllegalArgumentException ex) {
          throw errCtx.error()
              .message("Invalid date format string '%s'", input)
              .addContext("Details", ex.getMessage())
              .addContext("Input text", input)
              .build();
        }
    }

    public void eval() {

        // print current value in the desired format
        String str = format.print(left.value);

        out.buffer = buffer;
        out.start = 0;
        out.end = Math.min(100, str.length()); // truncate if target type has length smaller than that of input's string
        out.buffer.setBytes(0, str.substring(0,out.end).getBytes());
    }
}
</#list>
