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

<#list cast.types as type>

<#if type.major == "IntervalYearExactNumeric">  <#-- Template to convert from Interval, IntervalYear, IntervalDay to VarChar -->

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gcast/Cast${type.from}To${type.to}.java" />

<#include "/@includes/license.ftl" />

  package com.dremio.exec.expr.fn.impl.gcast;

<#include "/@includes/vv_imports.ftl" />

  import com.dremio.exec.expr.SimpleFunction;
  import com.dremio.exec.expr.annotations.FunctionTemplate;
  import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
  import com.dremio.exec.expr.annotations.Output;
  import com.dremio.exec.expr.annotations.Param;
  import org.apache.arrow.vector.holders.*;

  /**
   * generated from ${.template_name} ${type.from} ${type.to} ${type.major}
   */
@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}To${type.to} implements SimpleFunction {

  @Param ${type.from}Holder in;
  @Output ${type.to}Holder out;

  public void setup() {
  }

  public void eval() {
    out.value = in.value;
  }
}
</#if> <#-- type.major -->
</#list>
