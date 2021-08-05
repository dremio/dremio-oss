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

import com.dremio.exec.expr.annotations.Workspace;

<@pp.dropOutputFile />

<#list dateIntervalFunc.dates as first>
<#list dateIntervalFunc.dates as second>

<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/MonthsBetween${first}${second}.java" />

<#include "/@includes/license.ftl" />

  package com.dremio.exec.expr.fn.impl;

<#include "/@includes/vv_imports.ftl" />

  import static java.math.BigDecimal.ROUND_HALF_UP;
  import static java.util.Calendar.DATE;
  import static java.util.Calendar.HOUR_OF_DAY;
  import static java.util.Calendar.MINUTE;
  import static java.util.Calendar.MONTH;
  import static java.util.Calendar.SECOND;
  import static java.util.Calendar.YEAR;

  import com.dremio.exec.expr.SimpleFunction;
  import com.dremio.exec.expr.annotations.FunctionTemplate;
  import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
  import com.dremio.exec.expr.annotations.Output;
  import com.dremio.exec.expr.annotations.Workspace;
  import com.dremio.exec.expr.annotations.Param;
  import com.dremio.exec.expr.fn.FunctionErrorContext;
  import org.apache.arrow.vector.holders.*;

  import io.netty.buffer.ByteBuf;
  import org.apache.arrow.memory.ArrowBuf;
  import javax.inject.Inject;

/**
 * generated from ${.template_name} ${first} ${second}
 */
@SuppressWarnings("unused")
@FunctionTemplate(name = "months_between", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class MonthsBetween${first}${second} implements SimpleFunction {

    @Param  ${first}Holder left;
    @Param  ${second}Holder right;
    @Inject ArrowBuf buffer;
    @Workspace java.util.Calendar cal1;
    @Workspace java.util.Calendar cal2;
    @Output Float8Holder out;
    @Inject FunctionErrorContext errCtx;

    public void setup() {
      cal1 = new java.util.GregorianCalendar(java.util.TimeZone.getTimeZone("UTC".intern()));
      cal1.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
      cal2 = new java.util.GregorianCalendar(java.util.TimeZone.getTimeZone("UTC".intern()));
      cal2.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
    }

    public void eval() {
      cal1.setTimeInMillis(right.value);
      cal2.setTimeInMillis(left.value);

      // skip day/time part if both dates are end of the month
      // or the same day of the month
      int monDiffInt = (cal1.get(YEAR) - cal2.get(YEAR)) * 12 + (cal1.get(MONTH) - cal2.get(MONTH));
      if (cal1.get(DATE) == cal2.get(DATE)
          || (cal1.get(DATE) == cal1.getActualMaximum(DATE) && cal2.get(DATE) == cal2.getActualMaximum(DATE))) {
          out.value = (double)monDiffInt;
      }else{
          int sec1 = getDayPartInSec(cal1);
          int sec2 = getDayPartInSec(cal2);

          // 1 sec is 0.000000373 months (1/2678400). 1 month is 31 days.
          // there should be no adjustments for leap seconds
          double monBtwDbl = monDiffInt + (sec1 - sec2) / 2678400D;
          if (isRoundOffNeeded) {
              // Round a double to 8 decimal places.
              monBtwDbl = java.util.BigDecimal.valueOf(monBtwDbl)
                             .setScale(8, ROUND_HALF_UP)
                             .doubleValue();
          }

          out.value = monBtwDbl;
      }
    }

    protected int getDayPartInSec(java.util.Calendar cal) {
      int dd = cal.get(DATE);
      int HH = cal.get(HOUR_OF_DAY);
      int mm = cal.get(MINUTE);
      int ss = cal.get(SECOND);
      int dayInSec = dd * 86400 + HH * 3600 + mm * 60 + ss;
      return dayInSec;
    }
}
</#list>
