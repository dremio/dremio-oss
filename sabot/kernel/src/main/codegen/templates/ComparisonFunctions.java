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


/**
  NOTE: ComparisonFunctions.java does not contain/generate all comparison
  functions.  DecimalFunctions.java and DateIntervalFunctions.java contain
  some.
*/
<#-- TODO:  Refactor comparison code from DecimalFunctions.java and
     DateIntervalFunctions.java into here (to eliminate duplicate template code
     and so that ComparisonFunctions actually has all comparison functions. -->


<@pp.dropOutputFile />

<#include "/@includes/isDistinctFrom.ftl" />

<#macro compareNullsSubblock leftType rightType output breakTarget nullCompare nullComparesHigh>
  <#if nullCompare>
    <#if nullComparesHigh>
      <#assign leftNullResult = 1> <#-- if only L is null and nulls are high, then "L > R" (1) -->
      <#assign rightNullResult = -1>
    <#else>
      <#assign leftNullResult = -1> <#-- if only L is null and nulls are low, then "L < R" (-1) -->
      <#assign rightNullResult = 1>
    </#if>

    <#if leftType?starts_with("Nullable")>
      <#if rightType?starts_with("Nullable")>
        <#-- Both are nullable. -->
        if ( left.isSet == 0 ) {
          if ( right.isSet == 0 ) {
            <#-- Both are null--result is "L = R". -->
            ${output} = 0;
            break ${breakTarget};
          } else {
            <#-- Only left is null--result is "L < R" or "L > R" per null ordering. -->
            ${output} = ${leftNullResult};
            break ${breakTarget};
          }
        } else if ( right.isSet == 0 ) {
          <#-- Only right is null--result is "L > R" or "L < R" per null ordering. -->
          ${output} = ${rightNullResult};
          break ${breakTarget};
        }
      <#else>
        <#-- Left is nullable but right is not. -->
        if ( left.isSet == 0 ) {
          <#-- Only left is null--result is "L < R" or "L > R" per null ordering. -->
          ${output} = ${leftNullResult};
          break ${breakTarget};
        }
      </#if>
    <#elseif rightType?starts_with("Nullable")>
      <#-- Left is not nullable but right is. -->
      if ( right.isSet == 0 ) {
        <#-- Only right is null--result is "L > R" or "L < R" per null ordering. -->
        ${output} = ${rightNullResult};
        break ${breakTarget};
      }
    </#if>
  </#if>

</#macro>


<#-- macro compareBlock: Generates block handling comparison, including NULL. -->

<#-- Parameters: >
<#-- - mode: selects case of comparison code -->
<#-- - leftType: name of left argument's type  (e.g., NullableFloat4)  -->
<#-- - rightType: name of right argument's type  -->
<#-- - output: output variable name -->
<#-- - nullCompare: whether to generate null-comparison code -->
<#-- - nullComparesHigh:  whether NULL compares as the highest value or the lowest
       value -->

<#macro compareBlock mode leftType rightType output nullCompare nullComparesHigh>
     outside:
      {
        <@compareNullsSubblock leftType=leftType rightType=rightType
          output="out.value" breakTarget="outside" nullCompare=true nullComparesHigh=nullComparesHigh />

    <#if mode == "primitive">
      ${output} = left.value < right.value ? -1 : (left.value == right.value ? 0 : 1);
    <#elseif mode == "varString">
      ${output} = org.apache.arrow.vector.util.ByteFunctionHelpers.compare(
          left.buffer, left.start, left.end, right.buffer, right.start, right.end );
    <#elseif mode == "intervalNameThis">
      <@intervalCompareBlock leftType=leftType rightType=rightType
        leftMonths ="left.months"  leftDays ="left.days"  leftMillis ="left.milliseconds"
        rightMonths="right.months" rightDays="right.days" rightMillis="right.milliseconds"
        output="${output}"/>
    <#elseif mode == "intervalDay">
      <@intervalCompareBlock leftType=leftType rightType=rightType
        leftMonths ="0" leftDays ="left.days"  leftMillis ="left.milliseconds"
        rightMonths="0" rightDays="right.days" rightMillis="right.milliseconds"
        output="${output}"/>
       <#-- TODO:  Refactor other comparison code to here. -->
    <#else>
      ${mode_HAS_BAD_VALUE}
    </#if>

      } // outside
</#macro>

<#-- 1.  For each group of cross-comparable types: -->
<#list comparisonTypesMain.typeGroups as typeGroup>

<#-- 2.  For each pair of (cross-comparable) types in group: -->
<#list typeGroup.comparables as leftTypeBase>
<#list typeGroup.comparables as rightTypeBase>

<#-- Generate one file for each pair of base types (includes Nullable cases). -->
<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GCompare${leftTypeBase}Vs${rightTypeBase}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import org.apache.arrow.vector.util.ByteFunctionHelpers;
import org.apache.arrow.vector.holders.*;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;

/**
 * generated from ${.template_name} ${leftTypeBase} ${rightTypeBase}
 */
@SuppressWarnings("unused")
public class GCompare${leftTypeBase}Vs${rightTypeBase} {

<#-- 3.  For each combination of Nullable vs. not (of given non-nullable types):  -->
<#list ["Nullable${leftTypeBase}"] as leftType >
<#list ["Nullable${rightTypeBase}"] as rightType >


  <#-- Comparison function for sorting and grouping relational operators
       (not for comparison expression operators (=, <, etc.)). -->
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_HIGH,
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = NullHandling.INTERNAL)
  public static class GCompare${leftType}Vs${rightType}NullHigh implements SimpleFunction {

    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output NullableIntHolder out;

    public void setup() {}

    public void eval() {
      out.isSet = 1;
      <@compareBlock mode=typeGroup.mode leftType=leftType rightType=rightType
                     output="out.value" nullCompare=true nullComparesHigh=true />
    }
  }

  <#-- Comparison function for sorting and grouping relational operators
       (not for comparison expression operators (=, <, etc.)). -->
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_LOW,
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = NullHandling.INTERNAL)
  public static class GCompare${leftType}Vs${rightType}NullLow implements SimpleFunction {

    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output NullableIntHolder out;

    public void setup() {}

    public void eval() {
      out.isSet = 1;
      <@compareBlock mode=typeGroup.mode leftType=leftType rightType=rightType
                     output="out.value" nullCompare=true nullComparesHigh=false />
    }
  }

  <#-- IS_DISTINCT_FROM function -->
  @FunctionTemplate(names = {"is_distinct_from", "is distinct from" },
                    scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class GIsDistinctFrom${leftType}Vs${rightType} implements SimpleFunction {
    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output NullableBitHolder out;

    public void setup() {}

    public void eval() {
      <@isDistinctFromMacro leftType=leftType rightType=rightType mode=typeGroup.mode isNotDistinct=false/>
    }
  }

  <#-- IS_NOT_DISTINCT_FROM function -->
  @FunctionTemplate(names = {"is_not_distinct_from", "is not distinct from" },
                    scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class GIsNotDistinctFrom${leftType}Vs${rightType} implements SimpleFunction {
    @Param ${leftType}Holder left;
    @Param ${rightType}Holder right;
    @Output NullableBitHolder out;

    public void setup() {}

    public void eval() {
      <@isDistinctFromMacro leftType=leftType rightType=rightType mode=typeGroup.mode isNotDistinct=true/>
    }
  }

</#list>
</#list> <#-- 3. Nullable combinations -->



  <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
       not for sorting and grouping relational operators. -->
  @FunctionTemplate(names = {"less_than", "<"},
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class LessThan${leftTypeBase}Vs${rightTypeBase} implements SimpleFunction {

      @Param ${leftTypeBase}Holder left;
      @Param ${rightTypeBase}Holder right;
      @Output NullableBitHolder out;

      public void setup() {}

      public void eval() {

        <#if typeGroup.mode == "primitive">
          out.value = left.value < right.value ? 1 : 0;
        <#elseif typeGroup.mode == "varString"
            || typeGroup.mode == "intervalNameThis" || typeGroup.mode == "intervalDay" >
          int cmp;
          <@compareBlock mode=typeGroup.mode leftType=leftTypeBase rightType=rightTypeBase
                         output="cmp" nullCompare=false nullComparesHigh=false />
          out.value = cmp == -1 ? 1 : 0;
        <#-- TODO:  Refactor other comparison code to here. -->
        <#else>
          ${mode_HAS_BAD_VALUE}
        </#if>

      }
  }

  <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
       not for sorting and grouping relational operators. -->
  @FunctionTemplate(names = {"less_than_or_equal_to", "<="},
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class LessThanEq${leftTypeBase}Vs${rightTypeBase} implements SimpleFunction {

      @Param ${leftTypeBase}Holder left;
      @Param ${rightTypeBase}Holder right;
      @Output NullableBitHolder out;

      public void setup() {}

      public void eval() {

        <#if typeGroup.mode == "primitive">
          out.value = left.value <= right.value ? 1 : 0;
        <#elseif typeGroup.mode == "varString"
            || typeGroup.mode == "intervalNameThis" || typeGroup.mode == "intervalDay" >
          int cmp;
          <@compareBlock mode=typeGroup.mode leftType=leftTypeBase rightType=rightTypeBase
                         output="cmp" nullCompare=false nullComparesHigh=false />
          out.value = cmp < 1 ? 1 : 0;
        <#-- TODO:  Refactor other comparison code to here. -->
        <#else>
          ${mode_HAS_BAD_VALUE}
        </#if>

    }
  }

  <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
       not for sorting and grouping relational operators. -->
  @FunctionTemplate(names = {"greater_than", ">"},
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class GreaterThan${leftTypeBase}Vs${rightTypeBase} implements SimpleFunction {

      @Param ${leftTypeBase}Holder left;
      @Param ${rightTypeBase}Holder right;
      @Output NullableBitHolder out;

      public void setup() {}

      public void eval() {

        <#if typeGroup.mode == "primitive">
          out.value = left.value > right.value ? 1 : 0;
        <#elseif typeGroup.mode == "varString"
            || typeGroup.mode == "intervalNameThis" || typeGroup.mode == "intervalDay" >
          int cmp;
          <@compareBlock mode=typeGroup.mode leftType=leftTypeBase rightType=rightTypeBase
                         output="cmp" nullCompare=false nullComparesHigh=false />
          out.value = cmp == 1 ? 1 : 0;
        <#-- TODO:  Refactor other comparison code to here. -->
        <#else>
          ${mode_HAS_BAD_VALUE}
        </#if>

    }
  }

  <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
       not for sorting and grouping relational operators. -->
  @FunctionTemplate(names = {"greater_than_or_equal_to", ">="},
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class GreaterThanEq${leftTypeBase}Vs${rightTypeBase} implements SimpleFunction {

      @Param ${leftTypeBase}Holder left;
      @Param ${rightTypeBase}Holder right;
      @Output NullableBitHolder out;

      public void setup() {}

      public void eval() {

        <#if typeGroup.mode == "primitive">
          out.value = left.value >= right.value ? 1 : 0;
        <#elseif typeGroup.mode == "varString"
            || typeGroup.mode == "intervalNameThis" || typeGroup.mode == "intervalDay" >
          int cmp;
          <@compareBlock mode=typeGroup.mode leftType=leftTypeBase rightType=rightTypeBase
                         output="cmp" nullCompare=false nullComparesHigh=false />
          out.value = cmp > -1 ? 1 : 0;
        <#-- TODO:  Refactor other comparison code to here. -->
        <#else>
          ${mode_HAS_BAD_VALUE}
        </#if>

      }
  }

  <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
       not for sorting and grouping relational operators. -->
  @FunctionTemplate(names = {"equal", "==", "="},
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class Equals${leftTypeBase}Vs${rightTypeBase} implements SimpleFunction {

      @Param ${leftTypeBase}Holder left;
      @Param ${rightTypeBase}Holder right;
      @Output NullableBitHolder out;

      public void setup() {}

      public void eval() {

        <#if typeGroup.mode == "primitive">
          out.value = left.value == right.value ? 1 : 0;
        <#elseif typeGroup.mode == "varString" >
          out.value = org.apache.arrow.vector.util.ByteFunctionHelpers.equal(
              left.buffer, left.start, left.end, right.buffer, right.start, right.end);
        <#elseif typeGroup.mode == "intervalNameThis" || typeGroup.mode == "intervalDay" >
          int cmp;
          <@compareBlock mode=typeGroup.mode leftType=leftTypeBase rightType=rightTypeBase
                         output="cmp" nullCompare=false nullComparesHigh=false />
          out.value = cmp == 0 ? 1 : 0;
        <#-- TODO:  Refactor other comparison code to here. -->
        <#else>
          ${mode_HAS_BAD_VALUE}
        </#if>

      }
  }

  <#-- Comparison function for comparison expression operator (=, &lt;, etc.),
       not for sorting and grouping relational operators. -->
  @FunctionTemplate(names = {"not_equal", "<>", "!="},
                    scope = FunctionTemplate.FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class NotEquals${leftTypeBase}Vs${rightTypeBase} implements SimpleFunction {

      @Param ${leftTypeBase}Holder left;
      @Param ${rightTypeBase}Holder right;
      @Output NullableBitHolder out;

      public void setup() {}

      public void eval() {

        <#if typeGroup.mode == "primitive">
          out.value = left.value != right.value ? 1 : 0;
        <#elseif typeGroup.mode == "varString"
            || typeGroup.mode == "intervalNameThis" || typeGroup.mode == "intervalDay" >
          int cmp;
          <@compareBlock mode=typeGroup.mode leftType=leftTypeBase rightType=rightTypeBase
                         output="cmp" nullCompare=false nullComparesHigh=false />
          out.value = cmp == 0 ? 0 : 1;
        <#-- TODO:  Refactor other comparison code to here. -->
        <#else>
          ${mode_HAS_BAD_VALUE}
        </#if>

      }
  }

}


</#list> <#-- 2.  Pair of types-->
</#list>

</#list> <#-- 1. Group -->

