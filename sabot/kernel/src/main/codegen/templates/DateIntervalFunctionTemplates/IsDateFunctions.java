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

<#list dateIntervalFunc.dates as type>

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GIs${type}Functions.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import javax.inject.Inject;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;

import org.apache.arrow.vector.holders.*;

/**
 * generated from ${.template_name} ${type}
 * implementations of IS_${type?upper_case}(text, format) that checks if 'text' is a proper ${type?upper_case} if
 * formatted using the passed 'format' string.<br>
 * 'text' can be null, and the function returns FALSE in that case
 */
public class GIs${type}Functions {
      <#assign typeMapping = TypeMappings[type]!{}>
      <#assign dremioMinorType = typeMapping.minor_type!type?upper_case>
    @FunctionTemplate(name = "is_${dremioMinorType}" , scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
    public static class GIs${type}_Null implements SimpleFunction {

        @Param  
        NullableVarCharHolder left;
        
        @Param(constant=true)
        NullableVarCharHolder right;
        
        @Workspace 
        org.joda.time.format.DateTimeFormatter format;
        
        @Output 
        NullableBitHolder out;

        @Inject
        FunctionErrorContext errCtx;

        public void setup() {
            // Get the desired output format
            byte[] buf = new byte[right.end - right.start];
            right.buffer.getBytes(right.start, buf, 0, right.end - right.start);
            String formatString = new String(buf, java.nio.charset.StandardCharsets.UTF_8);
            format = com.dremio.exec.expr.fn.impl.DateFunctionsUtils.getFormatterForFormatString(formatString, errCtx);
        }

        public void eval() {
            out.isSet = 1;
            if (left.isSet == 1) {
                // Get the input
                byte[]buf1=new byte[left.end-left.start];
                left.buffer.getBytes(left.start,buf1,0,left.end-left.start);
                String input=new String(buf1, java.nio.charset.StandardCharsets.UTF_8);

                try{
                    com.dremio.exec.expr.fn.impl.DateFunctionsUtils.format${type}(input, format, errCtx);
                    out.value=1;
                } catch(Exception ex) {
                    // do not throw on purpose as we just want to set out.value
                }
            }
        }
    }

}
</#list>
