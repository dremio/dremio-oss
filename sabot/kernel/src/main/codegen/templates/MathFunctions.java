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


<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GMathFunctions.java" />

<#include "/@includes/license.ftl" />

/*
 * This class is automatically generated from AddTypes.tdd using FreeMarker.
 */


package com.dremio.exec.expr.fn.impl;


import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.impl.StringFunctions;
import org.apache.arrow.vector.holders.*;

@SuppressWarnings("unused")

public class GMathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GMathFunctions.class);
  
  private GMathFunctions(){}

  <#list mathFunc.unaryMathFunctions as func>

  <#list func.types as type>

  @FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}${type.input} implements SimpleFunction {

    @Param ${type.input}Holder in;
    @Output ${type.outputType}Holder out;

    public void setup() {
    }

    public void eval() {

      <#if func.funcName=='truncate'>
      <#if type.roundingRequired ??>
      java.math.BigDecimal bd = java.math.BigDecimal.valueOf(in.value).setScale(0, java.math.BigDecimal.ROUND_DOWN);
      out.value = <#if type.extraCast ??>(${type.extraCast})</#if>bd.${type.castType}Value();
      <#else>
      out.value = (${type.castType}) (in.value);</#if>
      <#else>
      out.value = (${type.castType}) ${func.javaFunc}(in.value);
      </#if>
    }
  }
  
  </#list>
  </#list>


  //////////////////////////////////////////////////////////////////////////////////////////////////
  //Math functions which take two arguments (of same type). 
  //////////////////////////////////////////////////////////////////////////////////////////////////
  
  <#list mathFunc.binaryMathFunctions as func>
  <#list func.types as type>

  @FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}${type.input} implements SimpleFunction {

    @Param ${type.input}Holder input1;
    @Param ${type.input}Holder input2;
    @Output ${type.outputType}Holder out;

    public void setup() {
    }

    public void eval() {
	<#if func.funcName == 'div'>
	<#if type.roundingRequired ??>
    java.math.BigDecimal bdOut = java.math.BigDecimal.valueOf(input1.value ${func.javaFunc} input2.value).setScale(0, java.math.BigDecimal.ROUND_DOWN);
    out.value = bdOut.${type.castType}Value();
    <#else>
    out.value = (${type.castType}) ( input1.value ${func.javaFunc} input2.value);
    </#if>
    <#else>
    out.value =(${type.castType}) ( input1.value ${func.javaFunc} input2.value);
    </#if>
    }
  }
  </#list>
  </#list>

  <#list mathFunc.otherMathFunctions as func>
  <#list func.types as type>

  @FunctionTemplate(<#if func.funcAlias ??>names = { "${func.funcName}", "${func.funcAlias}" }<#else>name = "${func.funcName}"</#if>, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}${type.dataType} implements SimpleFunction {

    @Param ${type.dataType}Holder input1;
    @Param IntHolder input2;
    @Output ${type.dataType}Holder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal temp = new java.math.BigDecimal(input1.value);
      out.value = (${type.castType}) temp.setScale(input2.value, java.math.RoundingMode.${func.mode}).doubleValue();
    }
  }
  </#list>
  </#list>
}



//////////////////////////////////////////////////////////////////////////////////////////////////
//End of GMath Functions
//////////////////////////////////////////////////////////////////////////////////////////////////




<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/ExtendedMathFunctions.java" />
<#include "/@includes/license.ftl" />

//////////////////////////////////////////////////////////////////////////////////////////////////
//Functions for Extended Math Functions
//////////////////////////////////////////////////////////////////////////////////////////////////


package com.dremio.exec.expr.fn.impl;

import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.impl.StringFunctions;
import org.apache.arrow.vector.holders.*;
/*
 * This class is automatically generated from MathFunc.tdd using FreeMarker.
 */

@SuppressWarnings("unused")

public class ExtendedMathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExtendedMathFunctions.class);

  private ExtendedMathFunctions(){}

//////////////////////////////////////////////////////////////////////////////////////////////////
//Unary Math functions with 1 parameter.
//////////////////////////////////////////////////////////////////////////////////////////////////

<#list mathFunc.extendedUnaryMathFunctions as func>

<#list func.types as type>

@FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public static class ${func.className}${type.input} implements SimpleFunction {

  @Param ${type.input}Holder in;
  @Output ${func.outputType}Holder out;

  public void setup() {
  }

  public void eval() {
	  <#if !type.input?matches("^Decimal")>
	  out.value = ${func.javaFunc}(in.value);
	  </#if>
  }
}

</#list>
</#list>


//////////////////////////////////////////////////////////////////////////////////////////////////
//Function to handle Log with base.
//////////////////////////////////////////////////////////////////////////////////////////////////
<#list mathFunc.logBaseMathFunction as func>
<#list func.types as type>

@FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public static class ${func.className}${type.input} implements SimpleFunction {

  @Param ${type.input}Holder base;
  @Param ${type.input}Holder val;
  @Output ${func.outputType}Holder out;

  public void setup() {
  }

  public void eval() {
	  <#if type.input?matches("^Decimal[1-9]*")>
	  double dblval = new java.math.BigDecimal(val.value).setScale(val.scale).doubleValue();
	  out.value = ${func.javaFunc}(dblval)/ ${func.javaFunc}(base.value);
	  <#else>
	  out.value = ${func.javaFunc}(val.value)/ ${func.javaFunc}(base.value);
	  </#if>
  }
}
</#list>
</#list>


}

//////////////////////////////////////////////////////////////////////////////////////////////////
//End of Extended Math Functions
//////////////////////////////////////////////////////////////////////////////////////////////////


<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/TrigoMathFunctions.java" />
<#include "/@includes/license.ftl" />

//////////////////////////////////////////////////////////////////////////////////////////////////
//Functions for Trigo Math Functions
//////////////////////////////////////////////////////////////////////////////////////////////////


package com.dremio.exec.expr.fn.impl;

import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.impl.StringFunctions;
import org.apache.arrow.vector.holders.*;

/*
 * This class is automatically generated from MathFunc.tdd using FreeMarker.
 */

@SuppressWarnings("unused")
/**
 * generated from ${.template_name} 
 */
public class TrigoMathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TrigoMathFunctions.class);

  private TrigoMathFunctions(){}

  <#list mathFunc.trigoMathFunctions as func>

  <#list func.types as type>

  @FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}${type.input} implements SimpleFunction {

    @Param ${type.input}Holder in;
    @Output ${func.outputType}Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = ${func.javaFunc}(in.value);
    }
  }
 </#list>
 </#list>

  <#list mathFunc.otherTrigoMathFunctions as func>

  <#list func.types as type>

  @FunctionTemplate(name = "${func.funcName}", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}${type.input} implements SimpleFunction {

    @Param ${type.input}Holder in1;
    @Param ${type.input}Holder in2;
    @Output ${func.outputType}Holder out;

    public void setup() {
    }

    public void eval() {
      out.value = ${func.javaFunc}(in1.value, in2.value);
    }
  }
 </#list>
 </#list>
}
