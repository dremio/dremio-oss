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

import java.lang.Override;

<@pp.dropOutputFile />

<#-- A utility class that is used to generate java code for single value aggregate functions -->

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gaggr/SingleValueFunctions.java" />

<#include "/@includes/license.ftl" />

// Source code generated using FreeMarker template ${.template_name}

package com.dremio.exec.expr.fn.impl.gaggr;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import javax.inject.Inject;
import org.apache.arrow.vector.holders.*;

@SuppressWarnings("unused")
/**
 * generated from ${.template_name} 
 */
public class SingleValueFunctions {
  <#list alltypes.types as type>
  @FunctionTemplate(name = "single_value", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class ${type.type}SingleValueFunction implements AggrFunction {

    @Param Nullable${type.type}Holder in;
    @Workspace Nullable${type.type}Holder value;
    @Workspace IntHolder isSet;
    @Output Nullable${type.type}Holder out;
    @Inject FunctionErrorContext errCtx;

    @Override
    public void setup() {
      isSet.value = 0;
    }

    @Override
    public void add() {
      if (isSet.value == 1) {
        throw errCtx.error()
          .message("Subqueries used in expressions must be scalar (must return a single value).")
          .build();
      }
      isSet.value = 1;
      value.isSet = in.isSet;
      <#if type.major == "VarBytes">
        value.start = in.start;
        value.end = in.end;
        value.buffer = in.buffer;
      <#elseif type.major == "Day">
        value.days = in.days;
        value.milliseconds = in.milliseconds;
      <#else>
        value.value = in.value;
      </#if>
    }

    @Override
    public void output() {
      out.isSet = value.isSet;
      <#if type.major == "VarBytes">
        out.start = value.start;
        out.end = value.end;
        out.buffer = value.buffer;
      <#elseif type.major == "Day">
        out.days = value.days;
        out.milliseconds = value.milliseconds;
      <#else>
        out.value = value.value;
      </#if>
    }

    @Override
    public void reset() {
      <#if type.major == "VarBytes">
        value.start = 0;
        value.end = 0;
        value.buffer = null;
      <#elseif type.major == "Day">
        value.days = 0;
        value.milliseconds = 0;
      <#else>
        value.value = 0;
      </#if>
      value.isSet = 0;
      isSet.value = 0;
    }
  }
  </#list>
}
