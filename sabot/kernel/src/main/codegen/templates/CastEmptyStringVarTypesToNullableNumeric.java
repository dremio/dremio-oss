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
<#if type.major == "EmptyString">

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/CastEmptyString${type.from}To${type.to}.java" />
<#include "/@includes/license.ftl" />

    package com.dremio.exec.expr.fn.impl.gcast;

    import com.dremio.exec.expr.SimpleFunction;
    import com.dremio.exec.expr.annotations.FunctionTemplate;
    import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
    import com.dremio.exec.expr.annotations.Output;
    import com.dremio.exec.expr.annotations.Param;
    import com.dremio.exec.expr.fn.FunctionErrorContext;
    import org.apache.arrow.vector.holders.*;
    import javax.inject.Inject;
    import io.netty.buffer.ArrowBuf;

/**
 * This file is generated with Freemarker using the template ${.template_name} ${type.from} ${type.to} ${type.major}
 */

@SuppressWarnings("unused")
@FunctionTemplate(name = "castEmptyString${type.from}To${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
public class CastEmptyString${type.from}To${type.to} implements SimpleFunction{

    @Param ${type.from}Holder in;
    @Output ${type.to}Holder out;
    @Inject FunctionErrorContext errCtx;

    public void setup() {}

    public void eval() {
    <#if type.to == "NullableFloat4" || type.to == "NullableFloat8">
        if(<#if type.from == "NullableVarChar" || type.from == "NullableVar16Char" ||
        type.from == "NullableVarBinary">in.isSet == 0 || </#if>in.end == in.start) {
            out.isSet = 0;
        } else{
            out.isSet = 1;
            byte[]buf=new byte[in.end-in.start];
            in.buffer.getBytes(in.start,buf,0,in.end-in.start);
            try{
              out.value=${type.javaType}.parse${type.parse}(new String(buf,com.google.common.base.Charsets.UTF_8));
            } catch (RuntimeException e) {
              throw errCtx.error(e)
                .build();
            }
        }
    <#elseif type.to=="NullableInt">
        if(<#if type.from == "NullableVarChar" || type.from == "NullableVar16Char" ||
            type.from == "NullableVarBinary">in.isSet == 0 || </#if>in.end == in.start) {
            out.isSet = 0;
        } else {
            out.isSet = 1;
            out.value = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.varTypesToInt(in.start, in.end, in.buffer, errCtx);
        }
    <#elseif type.to == "NullableBigInt">
        if(<#if type.from == "NullableVarChar" || type.from == "NullableVar16Char" ||
            type.from == "NullableVarBinary">in.isSet == 0 || </#if>
            in.end == in.start) {
            out.isSet = 0;
        } else {
            out.isSet = 1;
            out.value = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.varTypesToLong(in.start, in.end, in.buffer, errCtx);
        }
    </#if>
        }
}

</#if> <#-- type.major -->
</#list>
