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

<@pp.changeOutputFile name="/com/dremio/exec/store/dfs/implicit/ConstantColumnPopulators.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store.dfs.implicit;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.SuppressForbidden;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader.Populator;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import io.netty.util.internal.PlatformDependent;

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
    // This should be a multiple of 8 to allow copying validity buffers one after the other
    final static int NUM_PRE_FILLED_RECORDS = 4096; // pre-fill the value in these many records
    private NameValuePair pair;
    private ${minor.class}Vector vector;
    private ${minor.class}Vector tmpVector;
    private int elementSize;
    private int sizeOfPreFilledRecords;
    private List<AutoCloseable> closeableList = Lists.newArrayList();
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
      vector = (${minor.class}Vector)output.getVector(pair.getName());
      if (vector == null) {
        <#if minor.class == "Decimal">
        vector = output.addField(new Field(pair.getName(), new FieldType(true, new Decimal(value.precision, value.scale), null) , null), DecimalVector.class);
        <#else>
        vector = output.addField(CompleteType.${completeType}.toField(pair.name), ${minor.class}Vector.class);
        </#if>
      }
      closeableList.add(vector);
      elementSize = 0;
      if (value != null) {
        <#if minor.class == "Decimal">
        tmpVector = new ${minor.class}Vector(pair.getName(), vector.getAllocator(), value.precision, value.scale);
        <#else>
        tmpVector = new ${minor.class}Vector(pair.getName(), vector.getAllocator());
        </#if>
        for(int i = 0; i < NUM_PRE_FILLED_RECORDS; i++) {
        <#switch minor.class>
        <#case "Bit">
            ((${minor.class}Vector) tmpVector).setSafe(i, value ? 1 : 0);
        <#break>
        <#case "Int">
        <#case "BigInt">
        <#case "Float4">
        <#case "Float8">
        <#case "DateMilli">
        <#case "TimeMilli">
        <#case "TimeStampMilli">
            elementSize = vector.getTypeWidth();
            ((${minor.class}Vector) tmpVector).setSafe(i, value);
        <#break>
        <#case "Decimal">
            elementSize = vector.getTypeWidth();
            ((DecimalVector) tmpVector).setSafe(i, value);
        <#break>
        <#case "VarBinary">
        <#case "VarChar">
            elementSize = value.length;
            ((${minor.class}Vector) tmpVector).setSafe(i, value, 0, value.length);
        <#break>
        </#switch>
        }
        tmpVector.setValueCount(NUM_PRE_FILLED_RECORDS);
        sizeOfPreFilledRecords = tmpVector.getBufferSize();
        closeableList.add(tmpVector);
      }
    }

    public void populate(final int count){
      if ((count == 0) || (value == null)) {
        vector.setValueCount(count);
        return;
      }

      // allocate memory
      ValueVector valueVector = (ValueVector)vector;
      int numCopies = ((count - 1) / NUM_PRE_FILLED_RECORDS) + 1;
      ArrowBuf arrowBuf = vector.getAllocator().buffer(numCopies * sizeOfPreFilledRecords);

      int validityBufSize = (NUM_PRE_FILLED_RECORDS / 8);
      int endSize = validityBufSize * numCopies;
      ArrowBuf validityBuf = arrowBuf.slice(0, endSize);

      <#if minor.class == "Bit">
      int dataBufSize = NUM_PRE_FILLED_RECORDS / 8;
      <#else>
      int dataBufSize = NUM_PRE_FILLED_RECORDS * elementSize;
      </#if>

      ArrowBuf dataBuf = arrowBuf.slice(endSize, numCopies * dataBufSize);
      endSize += numCopies * dataBufSize;
      for(int i = 0; i < numCopies; i++) {
        validityBuf.setBytes(i * validityBufSize, tmpVector.getValidityBuffer(), 0, validityBufSize);
        dataBuf.setBytes(i * dataBufSize, tmpVector.getDataBuffer(), 0, dataBufSize);
      }

      ArrowFieldNode arrowFieldNode = new ArrowFieldNode(numCopies * NUM_PRE_FILLED_RECORDS, 0);
      <#if minor.class == "VarBinary" ||
           minor.class == "VarChar">
      ArrowBuf offsetBuf = null;
      if (valueVector instanceof BaseVariableWidthVector) {
        BaseVariableWidthVector baseVariableWidthVector = (BaseVariableWidthVector)vector;
        int offsetBufSize = (numCopies * NUM_PRE_FILLED_RECORDS + 1) * (BaseVariableWidthVector.OFFSET_WIDTH);
        int currentSize = 0;
        offsetBuf = arrowBuf.slice(endSize, offsetBufSize);
        for(int i = 0; i < offsetBufSize; i += 4, currentSize += elementSize) {
          offsetBuf.setInt(i, currentSize);
        }
        baseVariableWidthVector.loadFieldBuffers(arrowFieldNode, Lists.newArrayList(validityBuf, offsetBuf, dataBuf));
      }
      <#else>
      BaseFixedWidthVector baseFixedWidthVector = (BaseFixedWidthVector)vector;
      baseFixedWidthVector.loadFieldBuffers(arrowFieldNode, Lists.newArrayList(validityBuf, dataBuf));
      </#if>
      arrowBuf.close();
      vector.setValueCount(count);
    }

    public void allocate(){
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(closeableList);
    }

    @Override
    public String toString() {
      // return the info about the values than the actual values
      if (value == null) {
        return "${minor.class}:null";
      }
  <#switch minor.class>
  <#case "VarBinary">
  <#case "VarChar">
      return "${minor.class}:len:" + value.length;
  <#break>
  <#default>
      return "{minor.class}";
  </#switch>
    }
  }

  public static class ${minor.class}NameValuePair extends NameValuePair<${javaType}> {
    public ${minor.class}NameValuePair(String name, ${javaType} value) {
      super(name, value);
    }

    public Populator createPopulator(){
      return new ${minor.class}Populator(this);
    }

    public int getValueTypeSize() {
      <#switch minor.class>
      <#case "Bit">
      return -1;
      <#break>
      <#case "Int">
      <#case "Float4">
      <#case "TimeMilli">
      return 4;
      <#break>
      <#case "BigInt">
      <#case "Float8">
      <#case "DateMilli">
      <#case "TimeStampMilli">
      return 8;
      <#break>
      <#case "Decimal">
      return 16;
      <#break>
      <#case "VarBinary">
      <#case "VarChar">
      return Integer.MAX_VALUE;
      </#switch>
    }

    @SuppressForbidden
    public byte[] getValueBytes() {
      if (value != null) {
    <#switch minor.class >
    <#case "Bit" >
          byte[] arr = new byte[1];
          arr[0] = (byte) (value ? 1 : 0);
          return arr;
    <#break>
    <#case "Int" >
    <#case "TimeMilli" >
          byte[] arr = new byte[Integer.BYTES];
          PlatformDependent.putInt(arr, 0, value);
          return arr;
    <#break>
    <#case "BigInt" >
    <#case "DateMilli" >
    <#case "TimeStampMilli" >
          byte[] arr = new byte[Long.BYTES];
          PlatformDependent.putLong(arr, 0, value);
          return arr;
    <#break>
    <#case "Float4" >
          byte[] arr = new byte[Integer.BYTES];
          PlatformDependent.putInt(arr, 0, Float.floatToRawIntBits(value));
          return arr;
    <#break>
    <#case "Float8" >
          byte[] arr = new byte[Long.BYTES];
          PlatformDependent.putLong(arr, 0, Double.doubleToRawLongBits(value));
          return arr;
    <#break>
    <#case "Decimal" >
          byte[] arr = new byte[16];
          value.buffer.getBytes(0, arr);
          return arr;
    <#break>
    <#case "VarBinary" >
          return value;
    <#break>
    <#case "VarChar" >
          return value.getBytes(StandardCharsets.UTF_8);
    </#switch>
      } else {
        return null;
      }
    }
  }
</#if>
</#list>
</#list>
}

