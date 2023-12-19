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
<#if type.major == "SrcVarlen">

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/Cast${type.from}${type.to}Function.java" />

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
import org.apache.arrow.memory.ArrowBuf;

/**
 * generated from ${.template_name} ${type.from} ${type.to} ${type.major}
 */
public class Cast${type.from}${type.to}Function {
  @SuppressWarnings("unused")
  @FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class Cast${type.from}${type.to} implements SimpleFunction{

    @Param ${type.from}Holder in;
    @Output ${type.to}Holder out;
    @Inject FunctionErrorContext errCtx;

    public void setup() {}

    public void eval() {
      <#if type.to == "Float4" || type.to == "Float8">
        byte[] buf = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
        try{
          String inputValue = new String(buf, java.nio.charset.StandardCharsets.UTF_8);
          out.value = com.dremio.exec.expr.fn.impl.gcast.Cast${type.from}${type.to}Function.parse${type.javaType}(inputValue, false, 0);
        } catch (RuntimeException e) {
          throw errCtx.error(e)
            .build();
        }
      <#elseif type.to=="Int" >
        out.value = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.varTypesToInt(in.start, in.end, in.buffer, errCtx);

      <#elseif type.to == "BigInt">
        out.value = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.varTypesToLong(in.start, in.end, in.buffer, errCtx);
      </#if>
    }
  }

  <#if type.to == "Float4" || type.to == "Float8">
    public static ${type.javaType} parse${type.javaType}(String input, boolean minusSign, int firstCharIndex) {
      if (input.charAt(firstCharIndex) == '-'){
        minusSign = true;
        firstCharIndex++;
      } else if (input.charAt(firstCharIndex) == '+') {
        firstCharIndex++;
      }
      switch (input.charAt(firstCharIndex)) {
        case 'i':
        case 'I':
        case 'n':
        case 'N':
          switch (input.substring(firstCharIndex + 1).toLowerCase().trim()) {
            case "nfinity":
            case "nf":
              if (minusSign) {
                return ${type.javaType}.NEGATIVE_INFINITY;
              } else {
                return ${type.javaType}.POSITIVE_INFINITY;
              }
            case "an":
              return ${type.javaType}.NaN;
            default:
              throw new NumberFormatException("wrong input format");
          }
        default:
          return ${type.javaType}.parse${type.javaType}(input);
      }
    }
  </#if>
}
</#if> <#-- type.major -->
</#list>

