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



<#list aggrtypes3.aggrtypes as aggrtype>
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gaggr/${aggrtype.className}Functions.java" />

<#include "/@includes/license.ftl" />

// Source code generated using FreeMarker template ${.template_name}

<#-- A utility class that is used to generate java code for aggr functions such as stddev, variance -->

package com.dremio.exec.expr.fn.impl.gaggr;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;

@SuppressWarnings("unused")

/**
 * generated from ${.template_name} 
 */
public class ${aggrtype.className}Functions {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>

<#if aggrtype.aliasName == "">
@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
<#else>
@FunctionTemplate(names = {"${aggrtype.funcName}", "${aggrtype.aliasName}"}, scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
</#if>

public static class ${type.inputType}${aggrtype.className} implements AggrFunction{

  @Param ${type.inputType}Holder in;
  @Workspace ${type.movingAverageType}Holder avg;
  @Workspace ${type.movingDeviationType}Holder dev;
  @Workspace ${type.countRunningType}Holder count;
  @Workspace NullableBigIntHolder nonNullCount;
  @Output ${type.outputType}Holder out;

  public void setup() {
  	avg = new ${type.movingAverageType}Holder();
  	avg.value = 0;
    dev = new ${type.movingDeviationType}Holder();
    dev.value = 0;
    count = new ${type.countRunningType}Holder();
    count.value = 1;
  	nonNullCount = new NullableBigIntHolder();
  	nonNullCount.value = 0;
  	nonNullCount.isSet = 1;
    // Initialize the workspace variables
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
    // Welford's approach to compute standard deviation
    double temp = avg.value;
    avg.value += ((in.value - temp) / count.value);
    dev.value += (in.value - temp) * (in.value - avg.value);
    count.value++;

    <#if type.inputType?starts_with("Nullable")>
    } // end of sout block
    </#if>
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0) {
      out.isSet = 1;
      <#if aggrtype.funcName == "stddev_pop">
      if (count.value > 1)
        out.value = Math.sqrt((dev.value / (count.value - 1)));
      <#elseif aggrtype.funcName == "var_pop">
      if (count.value  > 1)
        out.value = (dev.value / (count.value - 1));
      <#elseif aggrtype.funcName == "stddev_samp">
      if (count.value  > 2)
        out.value = Math.sqrt((dev.value / (count.value - 2)));
      <#elseif aggrtype.funcName == "var_samp">
      if (count.value > 2)
        out.value = (dev.value / (count.value - 2));
      </#if>
     } else {
       out.isSet = 0;
    }
  }

  @Override
  public void reset() {
    nonNullCount.value = 0;
    avg.value = 0;
    dev.value = 0;
    count.value = 1;
  }
}


</#list>
}
</#list>
