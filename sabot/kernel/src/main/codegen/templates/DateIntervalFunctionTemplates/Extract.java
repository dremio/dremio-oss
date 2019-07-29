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
Test<@pp.dropOutputFile />
<#assign className="GExtract" />

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/${className}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.*;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.util.DateUtility;

/**
 * generated from ${.template_name}
 */
public class ${className} {

<#list dateIntervalFunc.extractInputTypes as fromUnit> <#-- Start InputType Loop -->
  <#list dateIntervalFunc.extractUnits as toUnit> <#-- Start UnitType Loop -->
    <#if fromUnit == "DateMilli" || fromUnit == "TimeMilli" || fromUnit == "TimeStampMilli">
      <#if !(fromUnit == "TimeMilli" &&
          (toUnit == "Year" || toUnit == "Month" || toUnit == "Day" ||
           toUnit == "Week" || toUnit == "DOW" || toUnit == "DOY" ||
           toUnit == "Quarter" || toUnit == "Decade" || toUnit == "Century" || toUnit == "Millennium"
          )
      )>
  @FunctionTemplate(name = "extract${toUnit}", scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ${toUnit}From${fromUnit} implements SimpleFunction {

    @Param ${fromUnit}Holder in;
    @Output BigIntHolder out;
    @Workspace org.joda.time.MutableDateTime dateTime;

    public void setup() {
      dateTime = new org.joda.time.MutableDateTime(
          org.joda.time.chrono.DayOfWeekFromSundayChronology.getISOInstanceInUTC());
    }

    public void eval() {
      dateTime.setMillis(in.value);

    <#if toUnit == "Second">
      out.value = dateTime.getSecondOfMinute();
      out.value += (dateTime.getMillisOfSecond()) / org.apache.arrow.vector.util.DateUtility.secondsToMillis;
    <#elseif toUnit = "Minute">
      out.value = dateTime.getMinuteOfHour();
    <#elseif toUnit = "Hour">
      out.value = dateTime.getHourOfDay();
    <#elseif toUnit = "Day">
      out.value = dateTime.getDayOfMonth();
    <#elseif toUnit = "Month">
      out.value = dateTime.getMonthOfYear();
    <#elseif toUnit = "Year">
      out.value = dateTime.getYear();
    <#elseif toUnit = "Week">
      out.value = dateTime.getWeekOfWeekyear();
    <#elseif toUnit = "DOW">
      out.value = dateTime.getDayOfWeek();
    <#elseif toUnit = "DOY">
      out.value = dateTime.getDayOfYear();
    <#elseif toUnit = "Epoch">
      out.value = dateTime.getMillis()/1000;
    <#elseif toUnit = "Quarter">
      out.value = (dateTime.getMonthOfYear()-1)/3 + 1;
    <#elseif toUnit = "Decade">
      out.value = dateTime.getYear()/10;
    <#elseif toUnit = "Century">
      out.value = (dateTime.getYear() - 1)/100 + 1;
    <#elseif toUnit = "Millennium">
      out.value = (dateTime.getYear() - 1)/1000 + 1;
    </#if>
    }
  }
      </#if>
    <#else>
    <#if toUnit != "Week" && toUnit != "DOW" && toUnit != "DOY">
  @FunctionTemplate(name = "extract${toUnit}", scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class ${toUnit}From${fromUnit} implements SimpleFunction {

    @Param ${fromUnit}Holder in;
    @Output BigIntHolder out;
    <#if toUnit == "Epoch">
    @Workspace org.joda.time.MutableDateTime dateTime;
    </#if>

    public void setup() {
      <#if toUnit == "Epoch">
      dateTime = new org.joda.time.MutableDateTime(
          org.joda.time.chrono.DayOfWeekFromSundayChronology.getISOInstanceInUTC());
      </#if>
    }

    public void eval() {
  <#if fromUnit == "Interval">
    <#if toUnit == "Year">
      out.value = (in.months / org.apache.arrow.vector.util.DateUtility.yearsToMonths);
    <#elseif toUnit == "Month">
      out.value = (in.months % org.apache.arrow.vector.util.DateUtility.yearsToMonths);
    <#elseif toUnit == "Day">
      out.value = in.days;
    <#elseif toUnit == "Hour">
      out.value = in.milliseconds/(org.apache.arrow.vector.util.DateUtility.hoursToMillis);
    <#elseif toUnit == "Minute">
      int millis = in.milliseconds % (org.apache.arrow.vector.util.DateUtility.hoursToMillis);
      out.value = millis / (org.apache.arrow.vector.util.DateUtility.minutesToMillis);
    <#elseif toUnit == "Second">
      long millis = in.milliseconds % org.apache.arrow.vector.util.DateUtility.minutesToMillis;
      out.value = millis / (org.apache.arrow.vector.util.DateUtility.secondsToMillis);
    <#elseif toUnit = "Epoch">
      dateTime.setMillis(0);
      dateTime.add(org.joda.time.DurationFieldType.years(), (in.months / org.apache.arrow.vector.util.DateUtility.yearsToMonths));
      dateTime.add(org.joda.time.DurationFieldType.months(), (in.months % org.apache.arrow.vector.util.DateUtility.yearsToMonths));
      dateTime.add(org.joda.time.DurationFieldType.days(), in.days);
      dateTime.add(org.joda.time.DurationFieldType.millis(), in.milliseconds);
      out.value = dateTime.getMillis()/1000;
    <#elseif toUnit = "Quarter">
      out.value = ((in.months % org.apache.arrow.vector.util.DateUtility.yearsToMonths)-1)/3 + 1;
    <#elseif toUnit = "Decade">
      out.value = (in.months / org.apache.arrow.vector.util.DateUtility.yearsToMonths)/10;
    <#elseif toUnit = "Century">
      out.value = (in.months / org.apache.arrow.vector.util.DateUtility.yearsToMonths)/100;
    <#elseif toUnit = "Millennium">
      out.value = (in.months / org.apache.arrow.vector.util.DateUtility.yearsToMonths) /1000;
    </#if>
  <#elseif fromUnit == "IntervalDay">
    <#if toUnit == "Day">
      out.value = in.days;
    <#elseif toUnit == "Hour">
      out.value = in.milliseconds/(org.apache.arrow.vector.util.DateUtility.hoursToMillis);
    <#elseif toUnit == "Minute">
      int millis = in.milliseconds % (org.apache.arrow.vector.util.DateUtility.hoursToMillis);
      out.value = millis / (org.apache.arrow.vector.util.DateUtility.minutesToMillis);
    <#elseif toUnit == "Second">
      long millis = in.milliseconds % org.apache.arrow.vector.util.DateUtility.minutesToMillis;
      out.value = millis / (org.apache.arrow.vector.util.DateUtility.secondsToMillis);
    <#elseif toUnit = "Epoch">
      dateTime.setMillis(0);
      dateTime.add(org.joda.time.DurationFieldType.days(), in.days);
      dateTime.add(org.joda.time.DurationFieldType.millis(), in.milliseconds);
      out.value = dateTime.getMillis()/1000;
    <#elseif toUnit = "Quarter">
      out.value = 1;
    <#else>
      out.value = 0;
    </#if>
  <#else> <#-- IntervalYear type -->
    <#if toUnit == "Year">
      out.value = (in.value / org.apache.arrow.vector.util.DateUtility.yearsToMonths);
    <#elseif toUnit == "Month">
      out.value = (in.value % org.apache.arrow.vector.util.DateUtility.yearsToMonths);
    <#elseif toUnit = "Epoch">
      dateTime.setMillis(0);
      dateTime.add(org.joda.time.DurationFieldType.years(), (in.value / org.apache.arrow.vector.util.DateUtility.yearsToMonths));
      dateTime.add(org.joda.time.DurationFieldType.months(), (in.value % org.apache.arrow.vector.util.DateUtility.yearsToMonths));
      out.value = dateTime.getMillis()/1000;
    <#elseif toUnit = "Quarter">
      out.value = ((in.value % org.apache.arrow.vector.util.DateUtility.yearsToMonths)-1)/3 + 1;
    <#elseif toUnit = "Decade">
      out.value = (in.value / org.apache.arrow.vector.util.DateUtility.yearsToMonths)/10;
    <#elseif toUnit = "Century">
      out.value = (in.value / org.apache.arrow.vector.util.DateUtility.yearsToMonths)/100;
    <#elseif toUnit = "Millennium">
      out.value = (in.value / org.apache.arrow.vector.util.DateUtility.yearsToMonths)/1000;
    <#else>
      out.value = 0;
    </#if>
  </#if>
    }
  }
</#if>
    </#if>
  </#list> <#-- End UnitType Loop -->
</#list> <#-- End InputType Loop -->
}
