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



<#list cast.types as type>
<#if type.major == "SrcVarlenTargetVarlen">

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/Cast${type.from}${type.to}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl.gcast;

import io.netty.buffer.ByteBuf;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import org.apache.arrow.vector.holders.*;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;

/**
 * generated from ${.template_name} ${type.from} ${type.to} ${type.major}
 */
@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}${type.to} implements SimpleFunction{

  @Param ${type.from}Holder in;
  @Param BigIntHolder length;
  @Output ${type.to}Holder out;
  @Inject FunctionErrorContext errCtx;

  public void setup() {
  }

  public void eval() {

  <#if type.to == 'VarChar'>

    //Do 1st scan to counter # of character in string.
    int charCount = com.dremio.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer), in.start, in.end, errCtx);

    //if input length <= target_type length, do nothing
    //else if target length = 0, it means cast wants all the characters in the input. Do nothing.
    //else truncate based on target_type length.
    out.buffer = in.buffer;
    out.start =  in.start;
    if (charCount <= length.value || length.value == 0 ) {
      out.end = in.end;
    } else {
      out.end = com.dremio.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer),
  in.start, in.end, (int)length.value, errCtx);
    }

  <#elseif type.to == 'VarBinary'>

    //if input length <= target_type length, do nothing
    //else if target length = 0, it means cast wants all the bytes in the input. Do nothing.
    //else truncate based on target_type length.
    out.buffer = in.buffer;
    out.start =  in.start;
    if (in.end - in.start <= length.value || length.value == 0 ) {
      out.end = in.end;
    } else {
      out.end = out.start + (int) length.value;
    }
  </#if>

  }      
}

</#if> <#-- type.major -->
</#list>

