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

<#list dateIntervalFunc.dates as type>

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GTo${type}.java" />

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
 */
public class GTo${type} {
  <#assign typeMapping = TypeMappings[type]!{}>
  <#assign dremioMinorType = typeMapping.minor_type!type?upper_case>
  @FunctionTemplate(name = "to_${dremioMinorType}" , scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class GVarCharTo${type} implements SimpleFunction{
    @Param VarCharHolder left;
    @Param VarCharHolder right;
    @Workspace org.joda.time.format.DateTimeFormatter format;
    @Workspace boolean replaceWithOffset;
    @Workspace int prevInputLength;
    @Workspace String prevTimezoneAbbr;
    @Output ${type}Holder out;
    @Inject FunctionErrorContext errCtx;

  public void setup(){
    // Get the desired output format
    byte[]buf=new byte[right.end-right.start];
    right.buffer.getBytes(right.start,buf,0,right.end-right.start);
    String formatString=new String(buf,java.nio.charset.StandardCharsets.UTF_8);
    if (formatString.toUpperCase().endsWith("TZD")) {
      formatString = formatString.substring(0, formatString.length() - 3) + "TZO";
      replaceWithOffset = true;
      prevInputLength = 0;
      prevTimezoneAbbr = "";
    }
    format=com.dremio.exec.expr.fn.impl.DateFunctionsUtils.getSQLFormatterForFormatString(formatString, errCtx);
  }

  public void eval(){
    // Get the input
    byte[]buf1=new byte[left.end-left.start];
    left.buffer.getBytes(left.start,buf1,0,left.end-left.start);
    String input=new String(buf1,java.nio.charset.StandardCharsets.UTF_8);
    if(replaceWithOffset) {
      String tzAbbr = "";
      if(prevInputLength == input.length() && input.endsWith(prevTimezoneAbbr)) {
        tzAbbr = prevTimezoneAbbr;
      } else {
        for(int i = input.length() - 1; i >= 0; i--) {
          char c = input.charAt(i);
          if( (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ) {
            tzAbbr =  String.valueOf(c) + tzAbbr;
          } else {
            break;
          }
        }
        prevInputLength = input.length();
        prevTimezoneAbbr = tzAbbr;
      }

      java.util.Optional<String> offset =  com.dremio.exec.store.sys.TimezoneAbbreviations.getOffset(tzAbbr);
      if(!offset.isPresent()) {
          throw errCtx.error()
          .message("Input text cannot be formatted to date")
          .addContext("Invalid timezone abbreviation", tzAbbr)
          .addContext("Input text", input)
          .build();
      }

      input = input.substring(0, input.length() - tzAbbr.length()) + offset.get();
    }

    out.value=com.dremio.exec.expr.fn.impl.DateFunctionsUtils.format${type}(input, format, errCtx);
  }
  }

<#list numericTypes.numeric as numerics>
  @FunctionTemplate(name = "to_${dremioMinorType}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class G${numerics}To${type} implements SimpleFunction {
    @Param  ${numerics}Holder left;
<#if numerics.startsWith("Decimal")>
    @Workspace java.math.BigInteger millisConstant;
</#if>
<#if type == "DateMilli" || type == "TimeMilli">
    @Workspace org.joda.time.MutableDateTime mutableDateTime;
</#if>
    @Output ${type}Holder out;
    @Inject FunctionErrorContext errCtx;

    public void setup() {
<#if numerics.startsWith("Decimal")>
      millisConstant = java.math.BigInteger.valueOf(1000);
</#if>
<#if type == "DateMilli" || type == "TimeMilli">
      mutableDateTime = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
</#if>
    }

    public void eval() {
      long inputMillis = 0;
<#if (numerics.startsWith("Decimal"))>
  <#if (numerics == "Decimal9") || (numerics == "Decimal18")>
      java.math.BigInteger value = java.math.BigInteger.valueOf(left.value);
      value = value.multiply(millisConstant);
      inputMillis = (new java.math.BigDecimal(value, left.scale)).longValue();
  <#elseif (numerics == "Decimal28Sparse") || (numerics == "Decimal38Sparse")>
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromSparse(left.buffer, left.start, left.nDecimalDigits, left.scale);
      inputMillis = input.multiply(millisConstant).longValue();
  </#if>
<#else>
      inputMillis = (long) (left.value * 1000l);
</#if>
<#if type=="DateMilli">
      try {
        mutableDateTime.setMillis(inputMillis);
      } catch (IllegalArgumentException e) {
        errCtx.error()
          .message("argument '%s' is not a valid milliseconds value", inputMillis)
          .build();
      }
      out.value = mutableDateTime.getMillis() - mutableDateTime.getMillisOfDay();
<#elseif type=="TimeStampMilli">
      out.value = inputMillis;
<#elseif type=="TimeMilli">
      try {
        mutableDateTime.setMillis(inputMillis);
      } catch (IllegalArgumentException e) {
        errCtx.error()
          .message("argument '%s' is not a valid milliseconds value", inputMillis)
          .build();
      }
      out.value = mutableDateTime.getMillisOfDay();
</#if>
    }
  }
</#list>

  @FunctionTemplate(name = "to_${dremioMinorType}" , scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class GVarCharTo${type}WithReplaceOptions implements SimpleFunction{
    @Param VarCharHolder left;
    @Param(constant=true) VarCharHolder patternHolder;
    @Param(constant=true) private NullableIntHolder replaceWithNullHolder;
    @Workspace org.joda.time.format.DateTimeFormatter format;
    @Workspace boolean replaceWithOffset;
    @Workspace int prevInputLength;
    @Workspace String prevTimezoneAbbr;
    @Output Nullable${type}Holder out;
    @Inject FunctionErrorContext errCtx;

    public void setup() {
      // Get the desired output format
      byte[] buf = new byte[patternHolder.end-patternHolder.start];
      patternHolder.buffer.getBytes(patternHolder.start, buf, 0, patternHolder.end-patternHolder.start);

      // if the format string is invalid we want to fail, so don't handle the exception
      String formatString = new String(buf, java.nio.charset.StandardCharsets.UTF_8);
      if (formatString.toUpperCase().endsWith("TZD")) {
        formatString = formatString.substring(0, formatString.length() - 3) + "TZO";
        replaceWithOffset = true;
        prevInputLength = 0;
        prevTimezoneAbbr = "";
      }
      format=com.dremio.exec.expr.fn.impl.DateFunctionsUtils.getSQLFormatterForFormatString(formatString, errCtx);
  }

    public void eval() {
      if (left.isSet == 0) {
        out.isSet = 0;
        return;
      }

      byte[] buf = new byte[left.end - left.start];
      left.buffer.getBytes(left.start, buf, 0, left.end - left.start);
      String input = new String(buf, java.nio.charset.StandardCharsets.UTF_8);

      if(replaceWithOffset) {
        String tzAbbr = "";
        if(prevInputLength == input.length() && input.endsWith(prevTimezoneAbbr)) {
          tzAbbr = prevTimezoneAbbr;
        } else {
          for(int i = input.length() - 1; i >= 0; i--) {
            char c = input.charAt(i);
            if( (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ) {
              tzAbbr =  String.valueOf(c) + tzAbbr;
            } else {
              break;
            }
          }
          prevInputLength = input.length();
          prevTimezoneAbbr = tzAbbr;
        }

        java.util.Optional<String> offset =  com.dremio.exec.store.sys.TimezoneAbbreviations.getOffset(tzAbbr);
        if(!offset.isPresent()) {
          throw errCtx.error()
            .message("Input text cannot be formatted to date")
            .addContext("Invalid timezone abbreviation", tzAbbr)
            .addContext("Input text", input)
            .build();
        }

        input = input.substring(0, input.length() - tzAbbr.length()) + offset.get();
      }

      try {
        out.value = com.dremio.exec.expr.fn.impl.DateFunctionsUtils.format${type}(input, format);
        out.isSet = 1;
      } catch (IllegalArgumentException e) {
        if (replaceWithNullHolder.value == 1) {
          out.isSet = 0;
        } else {
          // if replaceWithNullHolder is not set, we throw the exception to be consistent with to_${dremioMinorType}
          // calls without the replaceWithNullHolder argument
          throw errCtx.error()
            .message("Input text cannot be formatted to date")
            .addContext("Details", e.getMessage())
            .addContext("Input text", input)
            .build();
        }
      }
    }
  }
}

</#list>
