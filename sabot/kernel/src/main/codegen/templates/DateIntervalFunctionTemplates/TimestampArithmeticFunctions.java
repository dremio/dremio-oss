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
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/TimestampArithmeticFunctions.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.DateMilliHolder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.joda.time.MutableDateTime;

/**
* generated from ${.template_name}
*/
public class TimestampArithmeticFunctions {

<#list dateIntervalFunc.timestampAddInputTypes as inputUnit> <#-- Start InputType Loop -->
<#list dateIntervalFunc.timestampAddUnitsTypes as addUnitType> <#-- Start AddInputType Loop -->
<#list dateIntervalFunc.timestampAddUnits as addUnit> <#-- Start UnitType Loop -->
<#if !(inputUnit == "DateMilli" &&
    (addUnit == "MicroSecond" || addUnit == "Second" || addUnit == "Minute" || addUnit == "Hour"))>

  @FunctionTemplate(name = "timestampadd${addUnit}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class TimestampAdd${addUnitType}${addUnit}To${inputUnit} implements SimpleFunction {
    @Param ${addUnitType}Holder count;
    @Param ${inputUnit}Holder in;
    @Workspace MutableDateTime temp;
    @Workspace com.dremio.exec.util.TSI tsi;
    @Output ${inputUnit}Holder out;

    public void setup() {
      temp = new MutableDateTime(org.joda.time.DateTimeZone.UTC);
      tsi = com.dremio.exec.util.TSI.getByName("${addUnit?upper_case}");
    }

    public void eval() {
      temp.setMillis(in.value);
      tsi.addCount(temp, count.value);
      out.value = temp.getMillis();
    }
  }

</#if> <#-- filter -->
</#list> <#-- End UnitType Loop -->
</#list> <#-- End AddInputType Loop -->
</#list> <#-- End InputType Loop -->

<#list dateIntervalFunc.timestampDiffUnits as outputUnit> <#-- Start UnitType Loop -->

  @FunctionTemplate(name = "timestampdiff${outputUnit}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class TimestampDiff${outputUnit} implements SimpleFunction {
    @Param TimeStampMilliHolder left;
    @Param TimeStampMilliHolder right;
    @Workspace MutableDateTime temp;
    @Workspace com.dremio.exec.util.TSI tsi;
    @Output IntHolder out;

    public void setup() {
      temp = new MutableDateTime(org.joda.time.DateTimeZone.UTC);
      tsi = com.dremio.exec.util.TSI.getByName("${outputUnit?upper_case}");
    }

    public void eval() {
      out.value = (int) ((left.value > right.value) ?
                  -tsi.getDiff(new org.joda.time.Interval(right.value, left.value, org.joda.time.DateTimeZone.UTC)) :
                  tsi.getDiff(new org.joda.time.Interval(left.value, right.value, org.joda.time.DateTimeZone.UTC)));
    }
  }

</#list> <#-- End InputType Loop -->
}
