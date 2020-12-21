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

<#list typesOI.map as entry>
<#if entry.needOIForType == true>
<@pp.changeOutputFile name="//com/dremio/exec/expr/fn/impl/hive/${entry.type}${entry.hiveOI}.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl.hive;

import com.dremio.exec.expr.fn.impl.StringFunctionHelpers;

import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.holders.*;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class ${entry.type}${entry.hiveOI} {
<#assign seq = ["Required", "Optional"]>
<#list seq as mode>

  public static class ${mode} extends AbstractPrimitiveObjectInspector implements ${entry.hiveOI} {
    public ${mode}() {
      super(TypeInfoFactory.${entry.hiveType?lower_case}TypeInfo);
    }

<#if entry.type == "VarChar" && entry.hiveType == "VARCHAR">
    @Override
    public HiveVarcharWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarCharHolder h = (NullableVarCharHolder)o;
    <#else>
      final VarCharHolder h = (VarCharHolder)o;
    </#if>
      final HiveVarcharWritable valW = new HiveVarcharWritable();
      valW.set(StringFunctionHelpers.toStringFromUTF8(h.start, h.end, h.buffer), HiveVarchar.MAX_VARCHAR_LENGTH);
      return valW;
    }

    @Override
    public HiveVarchar getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarCharHolder h = (NullableVarCharHolder)o;
    <#else>
      final VarCharHolder h = (VarCharHolder)o;
    </#if>
      final String s = StringFunctionHelpers.toStringFromUTF8(h.start, h.end, h.buffer);
      return new HiveVarchar(s, HiveVarchar.MAX_VARCHAR_LENGTH);
    }
<#elseif entry.type == "VarChar" && entry.hiveType == "STRING">
    @Override
    public Text getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarCharHolder h = (NullableVarCharHolder)o;
    <#else>
      final VarCharHolder h = (VarCharHolder)o;
    </#if>
      return new Text(StringFunctionHelpers.toStringFromUTF8(h.start, h.end, h.buffer));
    }

    @Override
    public String getPrimitiveJavaObject(Object o){
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarCharHolder h = (NullableVarCharHolder)o;
    <#else>
      final VarCharHolder h = (VarCharHolder)o;
    </#if>
      return StringFunctionHelpers.toStringFromUTF8(h.start, h.end, h.buffer);
    }
<#elseif entry.type == "VarBinary">
    @Override
    public BytesWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarBinaryHolder h = (NullableVarBinaryHolder)o;
    <#else>
      final VarBinaryHolder h = (VarBinaryHolder)o;
    </#if>
      final byte[] buf = new byte[h.end-h.start];
      h.buffer.getBytes(h.start, buf, 0, h.end-h.start);
      return new BytesWritable(buf);
    }

    @Override
    public byte[] getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarBinaryHolder h = (NullableVarBinaryHolder)o;
    <#else>
      final VarBinaryHolder h = (VarBinaryHolder)o;
    </#if>
      final byte[] buf = new byte[h.end-h.start];
      h.buffer.getBytes(h.start, buf, 0, h.end-h.start);
      return buf;
    }
<#elseif entry.type == "Bit">
    @Override
    public boolean get(Object o) {
    <#if mode == "Optional">
      return ((NullableBitHolder)o).value == 0 ? false : true;
    <#else>
      return ((BitHolder)o).value == 0 ? false : true;
    </#if>
    }

    @Override
    public BooleanWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      return new BooleanWritable(((NullableBitHolder)o).value == 0 ? false : true);
    <#else>
      return new BooleanWritable(((BitHolder)o).value == 0 ? false : true);
    </#if>
    }

    @Override
    public Boolean getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      return new Boolean(((NullableBitHolder)o).value == 0 ? false : true);
    <#else>
      return new Boolean(((BitHolder)o).value == 0 ? false : true);
    </#if>
    }
<#elseif entry.type == "Decimal">
    @Override
    public HiveDecimal getPrimitiveJavaObject(Object o){
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableDecimalHolder h = (NullableDecimalHolder) o;
    <#else>
      final DecimalHolder h = (DecimalHolder) o;
    </#if>
      h.start = (h.start / org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      return HiveDecimal.create(DecimalUtility.getBigDecimalFromArrowBuf(h.buffer, LargeMemoryUtil.capAtMaxInt(h.start), h.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableDecimalHolder h = (NullableDecimalHolder) o;
    <#else>
      final DecimalHolder h = (DecimalHolder) o;
    </#if>
      h.start = (h.start / org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      return new HiveDecimalWritable(
          HiveDecimal.create(DecimalUtility.getBigDecimalFromArrowBuf(h.buffer, LargeMemoryUtil.capAtMaxInt(h.start), h.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH)));
    }

<#elseif entry.type == "TimeStampMilli">
    @Override
    public java.sql.Timestamp getPrimitiveJavaObject(Object o){
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableTimeStampMilliHolder h = (NullableTimeStampMilliHolder) o;
    <#else>
      final TimeStampMilliHolder h = (TimeStampMilliHolder) o;
    </#if>
      return new java.sql.Timestamp(h.value);
    }

    @Override
    public TimestampWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableTimeStampMilliHolder h = (NullableTimeStampMilliHolder) o;
    <#else>
      final TimeStampMilliHolder h = (TimeStampMilliHolder) o;
    </#if>
      return new TimestampWritable(new java.sql.Timestamp(h.value));
    }

<#elseif entry.type == "Date">
    @Override
    public java.sql.Date getPrimitiveJavaObject(Object o){
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableDateMilliHolder h = (NullableDateMilliHolder) o;
    <#else>
      final DateMilliHolder h = (DateMilliHolder) o;
    </#if>
      return new java.sql.Date(h.value);
    }

    @Override
    public DateWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableDateMilliHolder h = (NullableDateMilliHolder) o;
    <#else>
      final DateMilliHolder h = (DateMilliHolder) o;
    </#if>
      return new DateWritable(new java.sql.Date(h.value));
    }

<#else>
    @Override
    public ${entry.javaType} get(Object o){
    <#if mode == "Optional">
      return ((Nullable${entry.type}Holder)o).value;
    <#else>
      return ((${entry.type}Holder)o).value;
    </#if>
    }

<#if entry.type == "Int">
    @Override
    public Integer getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
    </#if>
      return new Integer(get(o));
    }
<#else>
    @Override
    public ${entry.javaType?cap_first} getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      return new ${entry.javaType?cap_first}(((Nullable${entry.type}Holder)o).value);
    <#else>
      return new ${entry.javaType?cap_first}(((${entry.type}Holder)o).value);
    </#if>
    }
</#if>

    @Override
    public ${entry.javaType?cap_first}Writable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final Nullable${entry.type}Holder h = (Nullable${entry.type}Holder) o;
    <#else>
      final ${entry.type}Holder h = (${entry.type}Holder)o;
    </#if>
      return new ${entry.javaType?cap_first}Writable(h.value);
    }
</#if>
  }
</#list>
}
</#if>
</#list>

