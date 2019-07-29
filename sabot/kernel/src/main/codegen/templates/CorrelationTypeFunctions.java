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



<#list corrTypes.correlationTypes as aggrtype>
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/gaggr/${aggrtype.className}Functions.java" />

<#include "/@includes/license.ftl" />

<#-- A utility class that is used to generate java code for corr/correlation aggr functions -->

/*
 * This class is automatically generated from CorrelationTypes.tdd using FreeMarker.
 */

package com.dremio.exec.expr.fn.impl.gaggr;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.Float4Holder;

@SuppressWarnings("unused")

public class ${aggrtype.className}Functions {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}Functions.class);

<#list aggrtype.types as type>

@FunctionTemplate(names = {"${aggrtype.funcName}", "${aggrtype.aliasName}"}, scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class ${type.inputType}${aggrtype.className} implements AggrFunction{

  @Param ${type.inputType}Holder xIn;
  @Param ${type.inputType}Holder yIn;

  @Workspace ${type.movingAverageType}Holder xMean;
  @Workspace ${type.movingAverageType}Holder yMean;
  @Workspace ${type.movingAverageType}Holder xyMean;

  @Workspace ${type.movingAverageType}Holder xDev;
  @Workspace ${type.movingAverageType}Holder yDev;

  @Workspace ${type.movingDeviationType}Holder covar;

  @Workspace ${type.countRunningType}Holder count;
  @Output Nullable${type.outputType}Holder out;

  public void setup() {
    xMean = new ${type.movingAverageType}Holder();
    yMean = new ${type.movingAverageType}Holder();
    xyMean = new ${type.movingDeviationType}Holder();
    xDev = new ${type.movingDeviationType}Holder();
    yDev = new ${type.movingDeviationType}Holder();
    count = new ${type.countRunningType}Holder();
    covar = new ${type.movingDeviationType}Holder();

    // Initialize the workspace variables
    xMean.value = 0;
    yMean.value = 0;
    xyMean.value = 0;
    xDev.value = 0;
    yDev.value = 0;
    count.value = 1;
    covar.value = 0;
  }

  @Override
  public void add() {
	<#if type.inputType?starts_with("Nullable")>
	  sout: {
	  if (xIn.isSet == 0 || yIn.isSet == 0) {
	   // processing nullable input and the value is null, so don't do anything...
	   break sout;
	  }
	</#if>

    // compute covariance
	double xOldMean = xMean.value, yOldMean = yMean.value;

    xMean.value += ((xIn.value - xMean.value) / count.value);
    yMean.value += ((yIn.value - yMean.value) / count.value);

    xDev.value += (xIn.value - xOldMean) * (xIn.value - xMean.value);
    yDev.value += (yIn.value - yOldMean) * (yIn.value - yMean.value);

    xyMean.value += ((xIn.value * yIn.value) - xyMean.value) / count.value;
    count.value++;
    <#if type.inputType?starts_with("Nullable")>
    } // end of sout block
    </#if>
  }

  @Override
  public void output() {
    double xVariance = (xDev.value / (count.value - 1));
    double yVariance = (yDev.value / (count.value - 1));
    double xyCovariance = (xyMean.value - (xMean.value * yMean.value));

    out.isSet = 1;
    out.value = xyCovariance / Math.sqrt((xVariance * yVariance));
  }

  @Override
  public void reset() {
    xMean.value = 0;
    yMean.value = 0;
    xyMean.value = 0;
    count.value = 1;
    covar.value = 0;
  }
}


</#list>
}
</#list>
