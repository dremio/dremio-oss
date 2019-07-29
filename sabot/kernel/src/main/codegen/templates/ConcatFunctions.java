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

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/ConcatFunctions.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import java.nio.charset.Charset;

import javax.inject.Inject;

import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;

import io.netty.buffer.ArrowBuf;

/**
 * Freemarker generated from template ${.template_name}
 */
public class ConcatFunctions {

  public static final int CONCAT_MAX_ARGS = 10;

  private ConcatFunctions() {}

<#list 2..10 as n>
  @FunctionTemplate(name = "concat", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class Concat${n} implements SimpleFunction {

  <#list 1..n as i>
    @Param NullableVarCharHolder arg${i};
  </#list>
    @Output NullableVarCharHolder out;
    @Inject ArrowBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      int size = 0;
  <#list 1..n as i>
      size += (arg${i}.end - arg${i}.start);
  </#list>
      buffer = buffer.reallocIfNeeded(size);
      out.buffer = buffer;
      out.start = out.end = 0;
      out.isSet = 1;

  <#list 1..n as i>
      if (arg${i}.isSet == 1) {
        for (int id = arg${i}.start; id < arg${i}.end; id++) {
          out.buffer.setByte(out.end++, arg${i}.buffer.getByte(id));
        }
      }
  </#list>
    }
  }
</#list>
}
