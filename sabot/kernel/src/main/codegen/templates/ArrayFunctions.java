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


<@pp.changeOutputFile name="/com/dremio/exec/expr/fn/impl/GArrayFunctions.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl;

import javax.inject.Inject;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.*;
/**
 * generated from ${.template_name}
 */
public class GArrayFunctions {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GArrayFunctions.class);

  private GArrayFunctions(){}

  <#list ArrayFunction.types as type>
  @SuppressWarnings("unused")
  @FunctionTemplate(names = "array_sum_${type.typeName}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ArraySum${type.typeName} implements SimpleFunction {

    @Param private FieldReader in;
    @Output private ${type.holderForSum} out;
    @Inject private FunctionErrorContext errCtx;
    @Workspace private ${type.holderForSum} sum;

    public void setup() {
    }

    public void eval() {
      if (!in.isSet() || in.readObject() == null) {
        out.isSet = 0;
        return;
      }
      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new UnsupportedOperationException(
          String.format("First parameter to array_sum must be a LIST. Was given: %s",
            in.getMinorType().toString()
          )
        );
      }
      if (!in.reader().getMinorType().equals(org.apache.arrow.vector.types.Types.MinorType.${type.arrowType})) {
        throw new UnsupportedOperationException(
          String.format("List of %s is not comparable with %s",
            in.reader().getMinorType().toString(), org.apache.arrow.vector.types.Types.MinorType.${type.arrowType}
          )
        );
      }
      sum.isSet = 1;
      sum.value = 0;
      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      while (listReader.next()){
        if (listReader.reader().readObject() != null){
          sum.value += listReader.reader().${type.readValue}();
        }
      }
      out.isSet = 1;
      out.value = sum.value;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = "array_max_${type.typeName}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ArrayMax${type.typeName} implements SimpleFunction {

    @Param private FieldReader in;
    @Output private ${type.holder} out;
    @Inject private FunctionErrorContext errCtx;
    @Workspace private ${type.holder} max;

    public void setup () {
    }

    public void eval () {
      if (!in.isSet() || in.readObject() == null) {
        out.isSet = 0;
        return;
      }
      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new UnsupportedOperationException(
          String.format("First parameter to array_max must be a LIST. Was given: %s", in.getMinorType().toString()));
      }
      if (!in.reader().getMinorType().equals(org.apache.arrow.vector.types.Types.MinorType.${type.arrowType})){
        throw new UnsupportedOperationException(String.format("List of %s is not comparable with %s",
            in.reader().getMinorType().toString(), org.apache.arrow.vector.types.Types.MinorType.${type.arrowType}));
      }

      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      if (listReader.size() == 0) {
        out.isSet = 0;
        return;
      }
      max.isSet = 1;
      max.value = ${type.initialMaxValue};
      while (listReader.next()){
        if (listReader.reader().readObject() == null){
          out.isSet = 0;
          return;
        }
        ${type.javaType} val = listReader.reader().${type.readValue}();
        if (val > max.value) {
          max.value = val;
        }
      }
      out.isSet = 1;
      out.value = max.value;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = "array_min_${type.typeName}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ArrayMin${type.typeName} implements SimpleFunction {

    @Param private FieldReader in;
    @Output private ${type.holder} out;
    @Inject private FunctionErrorContext errCtx;
    @Workspace private ${type.holder} min;

    public void setup () {
    }

    public void eval () {
      if (!in.isSet() || in.readObject() == null) {
        out.isSet = 0;
        return;
      }
      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new UnsupportedOperationException(
          String.format("First parameter to array_min must be a LIST. Was given: %s", in.getMinorType().toString()));
      }
      if (!in.reader().getMinorType().equals(org.apache.arrow.vector.types.Types.MinorType.${type.arrowType})){
        throw new UnsupportedOperationException(String.format("List of %s is not comparable with %s",
          in.reader().getMinorType().toString(), org.apache.arrow.vector.types.Types.MinorType.${type.arrowType}));
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      if (listReader.size() == 0) {
        out.isSet = 0;
        return;
      }
      min.isSet = 1;
      min.value = ${type.initialMinValue};
      while (listReader.next()){
        if (listReader.reader().readObject() == null){
          out.isSet = 0;
          return;
        }
        ${type.javaType} val = listReader.reader().${type.readValue}();
        if (val < min.value) {
          min.value = val;
        }
      }
      out.isSet = 1;
      out.value = min.value;
    }
  }

  </#list>
}
