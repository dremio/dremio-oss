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



<#list aggrtypes2.aggrtypes as aggrtype>
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gaggr/${aggrtype.className}IntervalTypeFunctions.java" />

<#include "/@includes/license.ftl" />

// Source code generated using FreeMarker template ${.template_name}

<#-- A utility class that is used to generate java code for aggr functions for the interval data types. It maintains a running sum  -->
<#-- and a running count.  For now, this includes: AVG. -->

package com.dremio.exec.expr.fn.impl.gaggr;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.*;

@SuppressWarnings("unused")

public class ${aggrtype.className}IntervalTypeFunctions {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>
<#if type.major == "Date">

@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)

public static class ${type.inputType}${aggrtype.className} implements AggrFunction{

  @Param ${type.inputType}Holder in;
  @Workspace ${type.sumRunningType}Holder sum;
  @Workspace ${type.countRunningType}Holder count;
  @Workspace BigIntHolder nonNullCount;
  @Output ${type.outputType}Holder out;

  public void setup() {
  	sum = new ${type.sumRunningType}Holder();
    count = new ${type.countRunningType}Holder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    sum.value = 0;
    count.value = 0;
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
	  <#if aggrtype.funcName == "avg">
    <#if type.inputType.endsWith("IntervalDay")>
    sum.value += (long) in.days * (org.apache.arrow.vector.util.DateUtility.daysToStandardMillis) +
                        in.milliseconds;
    <#else>
    sum.value += in.value;
    </#if>
    count.value++;

	  <#else>
	  // TODO: throw an error ?
	  </#if>
	<#if type.inputType?starts_with("Nullable")>
    } // end of sout block
	</#if>
  }

  @Override
  public void output() {
    if (nonNullCount.value > 0) {
      out.isSet = 1;
      double millis = sum.value / ((double) count.value);
      <#if type.inputType.endsWith("IntervalYear")>
      out.value = (int) (millis / org.apache.arrow.vector.util.DateUtility.monthsToMillis);
      <#elseif type.inputType.endsWith("IntervalDay")>
      out.days = (int) (millis / org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
      out.milliseconds = (int) (millis % org.apache.arrow.vector.util.DateUtility.daysToStandardMillis);
      </#if>
    } else {
      out.isSet = 0;
    }
  }

  @Override
  public void reset() {
    nonNullCount.value = 0;
    sum.value = 0;
    count.value = 0;
  }

 }

</#if>
</#list>
}
</#list>

