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

<@pp.changeOutputFile name="/com/dremio/exec/store/dfs/implicit/ConstantColumnPopulators.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store.dfs.implicit;

import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ValueVector.Mutator;
import org.apache.arrow.vector.holders.DecimalHolder;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader.Populator;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Charsets;

/**
 * generated from ${.template_name}
 * Constant value column ValueVector populators.
 */
public class ConstantColumnPopulators {

<#list vv.types as type>
<#list type.minor as minor>
<#if
    minor.class == "Bit" ||
    minor.class == "Int" ||
    minor.class == "BigInt" ||
    minor.class == "Float4" ||
    minor.class == "Float8" ||
    minor.class == "Decimal" ||
    minor.class == "DateMilli" ||
    minor.class == "TimeMilli" ||
    minor.class == "TimeStampMilli" ||
    minor.class == "VarBinary" ||
    minor.class == "VarChar">

  <#assign javaType = "" >
  <#assign completeType = "" >
  <#switch minor.class>
    <#case "Bit">
      <#assign javaType = "Boolean" >
      <#assign completeType = "BIT">
    <#break>
    <#case "Int">
      <#assign javaType = "Integer" >
      <#assign completeType = "INT">
    <#break>
    <#case "BigInt">
      <#assign javaType = "Long" >
      <#assign completeType = "BIGINT">
    <#break>
    <#case "Float4">
      <#assign javaType = "Float" >
      <#assign completeType = "FLOAT">
    <#break>
    <#case "Float8">
      <#assign javaType = "Double" >
      <#assign completeType = "DOUBLE">
    <#break>
    <#case "Decimal">
      <#assign javaType = "DecimalHolder">
    <#break>
    <#case "DateMilli">
      <#assign javaType = "Long" >
      <#assign completeType = "DATE">
    <#break>
    <#case "TimeMilli">
      <#assign javaType = "Integer" >
      <#assign completeType = "TIME">
    <#break>
    <#case "TimeStampMilli">
      <#assign javaType = "Long" >
      <#assign completeType = "TIMESTAMP">
    <#break>
    <#case "VarBinary">
      <#assign javaType = "byte[]" >
      <#assign completeType = "VARBINARY">
    <#break>
    <#case "VarChar">
      <#assign javaType = "String" >
      <#assign completeType = "VARCHAR">
    <#break>
  </#switch>

  private static final class ${minor.class}Populator implements Populator, AutoCloseable {
    private NameValuePair pair;
    private Nullable${minor.class}Vector vector;
    <#if minor.class == "VarChar">
    private byte[] value;
    <#else>
    private ${javaType} value;
    </#if>

    public ${minor.class}Populator(${minor.class}NameValuePair pair){
      this.pair = pair;
      <#if minor.class == "VarChar">
      this.value = pair.value == null ? null : pair.value.getBytes(Charsets.UTF_8);
      <#else>
      this.value = pair.value;
    </#if>
    }

    public void setup(OutputMutator output){
      vector = (Nullable${minor.class}Vector)output.getVector(pair.getName());
      if (vector == null) {
        <#if minor.class == "Decimal">
        vector = output.addField(new Field(pair.getName(), true, new Decimal(value.precision, value.scale), null), NullableDecimalVector.class);
        <#else>
        vector = output.addField(CompleteType.${completeType}.toField(pair.name), Nullable${minor.class}Vector.class);
        </#if>
      }
    }

    public void populate(final int count){
      for (int i = 0; i < count; i++) {
        if(value != null) {
      <#switch minor.class>
      <#case "Bit">
          ((Nullable${minor.class}Vector) vector).setSafe(i, value ? 1 : 0);
      <#break>
      <#case "Int">
      <#case "BigInt">
      <#case "Float4">
      <#case "Float8">
      <#case "DateMilli">
      <#case "TimeMilli">
      <#case "TimeStampMilli">
          ((Nullable${minor.class}Vector) vector).setSafe(i, value);
      <#break>
      <#case "Decimal">
          ((NullableDecimalVector) vector).setSafe(i, value);
      <#break>
      <#case "VarBinary">
      <#case "VarChar">
          ((Nullable${minor.class}Vector) vector).setSafe(i, value, 0, value.length);
      <#break>
      </#switch>
        }
      }
      vector.setValueCount(count);
    }

    public void allocate(){
      vector.allocateNew();
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(vector);
    }
  }

  public static class ${minor.class}NameValuePair extends NameValuePair<${javaType}> {
    public ${minor.class}NameValuePair(String name, ${javaType} value) {
      super(name, value);
    }

    public Populator createPopulator(){
      return new ${minor.class}Populator(this);
    }
  }
</#if>
</#list>
</#list>
}

