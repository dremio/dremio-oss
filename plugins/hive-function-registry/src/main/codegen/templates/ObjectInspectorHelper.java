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
<@pp.changeOutputFile name="//com/dremio/exec/expr/fn/impl/hive/ObjectInspectorHelper.java" />

<#include "/@includes/license.ftl" />

package com.dremio.exec.expr.fn.impl.hive;

import com.sun.codemodel.*;

import com.dremio.common.expression.CodeModelArrowHelper;
import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.DirectExpression;
import com.dremio.exec.expr.TypeHelper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.lang.UnsupportedOperationException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.Multimap;
import com.google.common.collect.ArrayListMultimap;

public class ObjectInspectorHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ObjectInspectorHelper.class);

  private static Multimap<MinorType, Class> OIMAP_REQUIRED = ArrayListMultimap.create();
  private static Multimap<MinorType, Class> OIMAP_OPTIONAL = ArrayListMultimap.create();
  static {
<#list typesOI.map as entry>
    <#if entry.needOIForType == true>
    <#assign minorType = entry.minorType!entry.type?upper_case>
    OIMAP_REQUIRED.put(MinorType.${minorType}, ${entry.type}${entry.hiveOI}.Required.class);
    OIMAP_OPTIONAL.put(MinorType.${minorType}, ${entry.type}${entry.hiveOI}.Optional.class);
    </#if>
</#list>
  }

  public static ObjectInspector getObjectInspector(DataMode mode, MinorType minorType, boolean varCharToStringReplacement) {
    try {
      if (mode == DataMode.REQUIRED) {
        if (OIMAP_REQUIRED.containsKey(minorType)) {
          if (varCharToStringReplacement && minorType == MinorType.VARCHAR) {
            return (ObjectInspector) ((Class) OIMAP_REQUIRED.get(minorType).toArray()[1]).newInstance();
          } else {
            return (ObjectInspector) ((Class) OIMAP_REQUIRED.get(minorType).toArray()[0]).newInstance();
          }
        }
      } else if (mode == DataMode.OPTIONAL) {
        if (OIMAP_OPTIONAL.containsKey(minorType)) {
          if (varCharToStringReplacement && minorType == MinorType.VARCHAR) {
            return (ObjectInspector) ((Class) OIMAP_OPTIONAL.get(minorType).toArray()[1]).newInstance();
          } else {
            return (ObjectInspector) ((Class) OIMAP_OPTIONAL.get(minorType).toArray()[0]).newInstance();
          }
        }
      } else {
        throw new UnsupportedOperationException("Repeated types are not supported as arguement to Hive UDFs");
      }
    } catch(InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Failed to instantiate ObjectInspector", e);
    }

    throw new UnsupportedOperationException(
        String.format("Type %s[%s] not supported as arguement to Hive UDFs", minorType.toString(), mode.toString()));
  }

  public static JBlock initReturnValueHolder(ClassGenerator<?> g, JCodeModel m, JVar returnValueHolder, ObjectInspector oi, MinorType returnType) {
    JBlock block = new JBlock(false, false);
    switch(oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        switch(poi.getPrimitiveCategory()) {
<#list typesOI.map as entry>
          case ${entry.hiveType}:{
            JType holderClass = CodeModelArrowHelper.getHolderType(m, returnType, DataMode.OPTIONAL);
            block.assign(returnValueHolder, JExpr._new(holderClass));

          <#if entry.hiveType == "VARCHAR" || entry.hiveType == "STRING" || entry.hiveType == "BINARY" || entry.hiveType == "CHAR">
            block.assign( //
                returnValueHolder.ref("buffer"), //
                JExpr
                  .direct("context")
                  .invoke("getManagedBuffer")
                  .invoke("reallocIfNeeded")
                    .arg(JExpr.lit(1024))
            );
          </#if>
            return block;
          }
</#list>
          default:
            throw new UnsupportedOperationException(String.format("Received unknown/unsupported type '%s'", poi.getPrimitiveCategory().toString()));
        }
      }

      case MAP:
      case LIST:
      case STRUCT:
      default:
        throw new UnsupportedOperationException(String.format("Received unknown/unsupported type '%s'", oi.getCategory().toString()));
    }
  }

  private static Map<PrimitiveCategory, MinorType> TYPE_HIVE2MINOR = new HashMap<>();
  static {
<#list typesOI.map as entry>
    <#assign minorType = entry.minorType!entry.type?upper_case>
    TYPE_HIVE2MINOR.put(PrimitiveCategory.${entry.hiveType}, MinorType.${minorType});
</#list>
  }

  public static MinorType getMinorType(ObjectInspector oi) {
    switch(oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        if (TYPE_HIVE2MINOR.containsKey(poi.getPrimitiveCategory())) {
          return TYPE_HIVE2MINOR.get(poi.getPrimitiveCategory());
        }
        throw new UnsupportedOperationException();
      }

      case MAP:
      case LIST:
      case STRUCT:
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static JBlock getObject(JCodeModel m, ObjectInspector oi,
    JVar returnOI, JVar returnValueHolder, JVar returnValue) {
    JBlock block = new JBlock(false, false);
    switch(oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        switch(poi.getPrimitiveCategory()) {
<#list typesOI.map as entry>
          case ${entry.hiveType}:{
            JConditional jc = block._if(returnValue.eq(JExpr._null()));
            jc._then().assign(returnValueHolder.ref("isSet"), JExpr.lit(0));
            jc._else().assign(returnValueHolder.ref("isSet"), JExpr.lit(1));
            JVar castedOI = jc._else().decl(
              m.directClass(${entry.hiveOI}.class.getCanonicalName()), "castOI", JExpr._null());
            jc._else().assign(castedOI,
              JExpr.cast(m.directClass(${entry.hiveOI}.class.getCanonicalName()), returnOI));

          <#if entry.hiveType == "BOOLEAN">
            JConditional booleanJC = jc._else()._if(castedOI.invoke("get").arg(returnValue));
            booleanJC._then().assign(returnValueHolder.ref("value"), JExpr.lit(1));
            booleanJC._else().assign(returnValueHolder.ref("value"), JExpr.lit(0));

          <#elseif entry.hiveType == "VARCHAR" || entry.hiveType == "CHAR" || entry.hiveType == "STRING" || entry.hiveType == "BINARY">
            <#if entry.hiveType == "VARCHAR">
              JVar data = jc._else().decl(m.directClass(byte[].class.getCanonicalName()), "data",
                  castedOI.invoke("getPrimitiveJavaObject").arg(returnValue)
                      .invoke("getValue")
                      .invoke("getBytes"));
            <#elseif entry.hiveType == "CHAR">
                JVar data = jc._else().decl(m.directClass(byte[].class.getCanonicalName()), "data",
                    castedOI.invoke("getPrimitiveJavaObject").arg(returnValue)
                        .invoke("getStrippedValue")
                        .invoke("getBytes"));
            <#elseif entry.hiveType == "STRING">
              JVar data = jc._else().decl(m.directClass(byte[].class.getCanonicalName()), "data",
                  castedOI.invoke("getPrimitiveJavaObject").arg(returnValue)
                      .invoke("getBytes"));
            <#elseif entry.hiveType == "BINARY">
                JVar data = jc._else().decl(m.directClass(byte[].class.getCanonicalName()), "data",
                    castedOI.invoke("getPrimitiveJavaObject").arg(returnValue));
            </#if>

            JConditional jnullif = jc._else()._if(data.eq(JExpr._null()));
            jnullif._then().assign(returnValueHolder.ref("isSet"), JExpr.lit(0));

            jnullif._else().assign(returnValueHolder.ref("buffer"),
                returnValueHolder.ref("buffer").invoke("reallocIfNeeded").arg(data.ref("length")));
            jnullif._else().add(returnValueHolder.ref("buffer")
                .invoke("setBytes").arg(JExpr.lit(0)).arg(data));
            jnullif._else().assign(returnValueHolder.ref("start"), JExpr.lit(0));
            jnullif._else().assign(returnValueHolder.ref("end"), data.ref("length"));
            jnullif._else().add(returnValueHolder.ref("buffer").invoke("setIndex").arg(JExpr.lit(0)).arg(data.ref("length")));

          <#elseif entry.hiveType == "TIMESTAMP">
            JVar tsVar = jc._else().decl(m.directClass(java.sql.Timestamp.class.getCanonicalName()), "ts",
              castedOI.invoke("getPrimitiveJavaObject").arg(returnValue));
            jc._else().assign(returnValueHolder.ref("value"), tsVar.invoke("getTime"));
          <#elseif entry.hiveType == "DATE">
            JVar dVar = jc._else().decl(m.directClass(java.sql.Date.class.getCanonicalName()), "d",
              castedOI.invoke("getPrimitiveJavaObject").arg(returnValue));
            jc._else().assign(returnValueHolder.ref("value"), dVar.invoke("getTime"));
          <#else>
            jc._else().assign(returnValueHolder.ref("value"),
              castedOI.invoke("get").arg(returnValue));
          </#if>
            return block;
          }

</#list>
          default:
            throw new UnsupportedOperationException(String.format("Received unknown/unsupported type '%s'", poi.getPrimitiveCategory().toString()));
        }
      }

      case MAP:
      case LIST:
      case STRUCT:
      default:
        throw new UnsupportedOperationException(String.format("Received unknown/unsupported type '%s'", oi.getCategory().toString()));
    }
  }
}
