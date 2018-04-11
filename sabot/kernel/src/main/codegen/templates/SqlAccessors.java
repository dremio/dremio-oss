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

import java.lang.Override;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
  <#assign typeMapping = TypeMappings[minor.class]!{}>
  <#assign supported = typeMapping.supported!true>
  <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
<#if supported>
<#list ["", "Nullable"] as mode>
<#assign name = mode + minor.class?cap_first />
<#assign javaType = (minor.javaType!type.javaType) />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
<#-- Class returned by ResultSet.getObject(...): -->
<#assign jdbcObjectClass = minor.jdbcObjectClass ! friendlyType />

<@pp.changeOutputFile name="/com/dremio/exec/vector/accessor/${name}Accessor.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.vector.accessor;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.*;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.DremioStringUtils;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBufInputStream;
import com.dremio.common.util.DremioStringUtils;
import com.dremio.exec.vector.accessor.sql.TimePrintMillis;
import org.joda.time.Period;
import org.joda.time.LocalDateTime;

/**
 * generated from ${.template_name}
 */
@SuppressWarnings("unused")
public class ${name}Accessor extends AbstractSqlAccessor {
 
 <#if mode == "Nullable">
  private static final MajorType TYPE = Types.optional(MinorType.${dremioMinorType});
 <#else>
  private static final MajorType TYPE = Types.required(MinorType.${dremioMinorType});
 </#if>

  <#if mode == "Nullable">
  private final ${name}Vector ac;
  <#else>
  private final ${name}Vector.Accessor ac;
  </#if>

  public ${name}Accessor(${name}Vector vector) {
    <#if mode == "Nullable">
    this.ac = vector;
    <#else>
    this.ac = vector.getAccessor();
    </#if>
  }

  @Override
  public MajorType getType() {
    return TYPE;
  };

  @Override
  public boolean isNull(int index) {
   <#if mode == "Nullable">
    return ac.isNull(index);
   <#else>
    return false;
   </#if>
  }

 <#if minor.class != "VarChar" && minor.class != "TimeStampMilli"
   && minor.class != "TimeMilli" && minor.class != "DateMilli"
   && minor.class != "IntervalDay" && minor.class != "IntervalYear">
  <#-- Types whose class for JDBC getObject(...) is same as class from getObject
       on vector. -->

  @Override
  public Class<?> getObjectClass() {
    return ${jdbcObjectClass}.class;
  }

  @Override
  public Object getObject(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    return ac.getObject(index);
  }
 </#if>

 <#if type.major == "VarLen">

  @Override
  public InputStream getStream(int index) {
    <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    ${name}Holder h = new ${name}Holder();
    ac.get(index, h);
    return new ByteBufInputStream(h.buffer.slice(h.start, h.end));
  }

  @Override
  public byte[] getBytes(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    return ac.get(index);
  }

  <#switch minor.class>

<#case "Decimal">

  @Override
  public String getString(int index) {
<#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
</#if>
      return ac.getObject(index).toString();
    }
<#break>
    <#case "VarBinary">

    @Override
    public String getString(int index) {
     <#if mode == "Nullable">
      if (ac.isNull(index)) {
        return null;
      }
     </#if>
      byte [] b = ac.get(index);
      return DremioStringUtils.toBinaryString(b);
    }
      <#break>

    <#case "VarChar">

    @Override
    public Class<?> getObjectClass() {
      return String.class;
    }

    @Override
    public String getObject(int index) {
     <#if mode == "Nullable">
       if (ac.isNull(index)) {
         return null;
       }
     </#if>
       return getString(index);
    }

    @Override
    public InputStreamReader getReader(int index) {
     <#if mode == "Nullable">
      if (ac.isNull(index)) {
        return null;
      }
     </#if>
      return new InputStreamReader(getStream(index), Charsets.UTF_8);
    }

    @Override
    public String getString(int index) {
     <#if mode == "Nullable">
      if (ac.isNull(index)) {
        return null;
      }
     </#if>
      return new String(getBytes(index), Charsets.UTF_8);
    }
      <#break>

    <#case "Var16Char">

    @Override
    public InputStreamReader getReader(int index) {
     <#if mode == "Nullable">
      if (ac.isNull(index)) {
        return null;
      }
     </#if>
      return new InputStreamReader(getStream(index), Charsets.UTF_16);
    }

    @Override
    public String getString(int index) {
     <#if mode == "Nullable">
      if (ac.isNull(index)) {
        return null;
      }
     </#if>
      return new String(getBytes(index), Charsets.UTF_16);
    }
      <#break>

    <#default>
    This is uncompilable code

  </#switch>

 <#else> <#-- VarLen -->

  <#if minor.class == "TimeStampTZ">

  @Override
  public Class<?> getObjectClass() {
    return Timestamp.class;
  }

  @Override
  public Object getObject(int index) {
    return getTimestamp(index);
  }

  @Override
  public Timestamp getTimestamp(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    return new Timestamp(ac.getObject(index).getMillis());
  }

  <#elseif minor.class == "IntervalDay" || minor.class == "IntervalYear">

  @Override
  public Class<?> getObjectClass() {
        return String.class;
  }

  <#if minor.class == "IntervalDay">
  @Override
  public Object getObject(int index) {
  <#if mode == "Nullable">
        if (ac.isNull(index)) {
        return null;
        }
  </#if>
        return DremioStringUtils.formatIntervalDay(ac.getObject(index));
  }
  <#else>
@Override
public Object getObject(int index) {
<#if mode == "Nullable">
        if (ac.isNull(index)) {
        return null;
        }
</#if>
        return DremioStringUtils.formatIntervalYear(ac.getObject(index));
        }
  </#if>

  @Override
  public String getString(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    return String.valueOf(ac.getAsStringBuilder(index));
  }

  <#elseif minor.class.startsWith("Decimal")>

  @Override
  public BigDecimal getBigDecimal(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
      return ac.getObject(index);
  }
  <#elseif minor.class == "DateMilli">

  @Override
  public Class<?> getObjectClass() {
    return Date.class;
  }

  @Override
  public Object getObject(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    return getDate(index);
  }

  @Override
  public Date getDate(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    org.joda.time.LocalDateTime date = new org.joda.time.LocalDateTime(ac.get(index), org.joda.time.DateTimeZone.UTC);
    return new Date(date.getYear() - 1900, date.getMonthOfYear() - 1, date.getDayOfMonth());
  }

  <#elseif minor.class == "TimeStampMilli">

  @Override
  public Class<?> getObjectClass() {
    return Timestamp.class;
  }

  @Override
  public Object getObject(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    return getTimestamp(index);
  }

  @Override
  public Timestamp getTimestamp(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    org.joda.time.LocalDateTime date = new org.joda.time.LocalDateTime(ac.get(index), org.joda.time.DateTimeZone.UTC);
    return new Timestamp(date.getYear() - 1900,
                         date.getMonthOfYear() - 1,
                         date.getDayOfMonth(),
                         date.getHourOfDay(),
                         date.getMinuteOfHour(),
                         date.getSecondOfMinute(),
                         (int) java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(date.getMillisOfSecond()));
  }

  <#elseif minor.class == "TimeMilli">

  @Override
  public Class<?> getObjectClass() {
    return Time.class;
  }

  @Override
  public Object getObject(int index) {
    return getTime(index);
  }

  @Override
  public Time getTime(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
   </#if>
    org.joda.time.LocalDateTime time = new org.joda.time.LocalDateTime(ac.get(index), org.joda.time.DateTimeZone.UTC);
    return new TimePrintMillis(time);
  }

  <#else>

  @Override
  public ${javaType} get${javaType?cap_first}(int index) {
    return ac.get(index);
  }
  </#if>


  <#if minor.class == "Bit" >
  public boolean getBoolean(int index) {
   <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return false;
    }
   </#if>
   return 1 == ac.get(index);
  }
 </#if>


 </#if> <#-- not VarLen -->

}

</#list>
</#if>
</#list>
</#list>
