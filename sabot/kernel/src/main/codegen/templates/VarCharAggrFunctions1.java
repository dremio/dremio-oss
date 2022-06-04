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



<#list aggrtypes1.aggrtypes as aggrtype>
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gaggr/${aggrtype.className}VarBytesFunctions.java" />

<#include "/@includes/license.ftl" />

// Source code generated using FreeMarker template ${.template_name}

<#-- A utility class that is used to generate java code for aggr functions that maintain a single -->
<#-- running counter to hold the result.  This includes: MIN, MAX, COUNT. -->

package com.dremio.exec.expr.fn.impl.gaggr;

<#include "/@includes/vv_imports.ftl" />

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.holders.*;
import javax.inject.Inject;

import io.netty.buffer.ByteBuf;

@SuppressWarnings("unused")

public class ${aggrtype.className}VarBytesFunctions {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>
<#if type.major == "VarBytes">

@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class ${type.inputType}${aggrtype.className} implements AggrFunction{

  @Param ${type.inputType}Holder in;
  @Workspace ObjectHolder value;
  @Workspace NullableIntHolder init;
  @Workspace BigIntHolder nonNullCount;
  @Inject ArrowBuf buf;
  @Output ${type.outputType}Holder out;

  public void setup() {
    init = new NullableIntHolder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    init.value = 0;
    value = new ObjectHolder();
    com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = new com.dremio.exec.expr.fn.impl.ByteArrayWrapper();
    value.obj = tmp;
  }

  @Override
  public void add() {
	  <#if type.inputType?starts_with("Nullable")>
    sout: {
      if (in.isSet == 0) {
        // processing nullable input and the value is null, so don't do anything...
        break sout;
      }
    </#if>
    nonNullCount.value = 1;
    com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = (com.dremio.exec.expr.fn.impl.ByteArrayWrapper) value.obj;
    int cmp = 0;
    boolean swap = false;

    // if buffer is null then swap
    if (init.value == 0) {
      init.value = 1;
      swap = true;
    } else {
      // Compare the bytes
      cmp = org.apache.arrow.memory.util.ByteFunctionHelpers.compare(in.buffer, in.start, in.end, tmp.getBytes(), 0, tmp.getLength());


      <#if aggrtype.className == "Min">
      swap = (cmp == -1);
      <#elseif aggrtype.className == "Max">
      swap = (cmp == 1);
      </#if>
    }
    if (swap) {
      int inputLength = in.end - in.start;
      if (tmp.getLength() >= inputLength) {
        in.buffer.getBytes(in.start, tmp.getBytes(), 0, inputLength);
        tmp.setLength(inputLength);
      } else {
        byte[] tempArray = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, tempArray, 0, in.end - in.start);
        tmp.setBytes(tempArray);
      }
    }
    <#if type.inputType?starts_with("Nullable")>
    } // end of sout block
	  </#if>
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0) {
      out.isSet = 1;
      com.dremio.exec.expr.fn.impl.ByteArrayWrapper tmp = (com.dremio.exec.expr.fn.impl.ByteArrayWrapper) value.obj;
      buf = buf.reallocIfNeeded(tmp.getLength());
      buf.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
      out.start  = 0;
      out.end    = tmp.getLength();
      out.buffer = buf;
    } else {
      out.isSet = 0;
    }
  }

  @Override
  public void reset() {
    value = new ObjectHolder();
    value.obj = new com.dremio.exec.expr.fn.impl.ByteArrayWrapper();
    init.value = 0;
    nonNullCount.value = 0;
  }
}
</#if>
</#list>
}
</#list>
