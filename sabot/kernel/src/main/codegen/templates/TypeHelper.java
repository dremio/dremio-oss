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
<@pp.changeOutputFile name="/com/dremio/exec/expr/TypeHelper.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.expr;

import com.google.common.base.Preconditions;
import io.netty.buffer.*;

import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;

import com.dremio.exec.proto.UserBitShared.SerializedField;
import org.apache.arrow.vector.util.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.*;
import org.apache.arrow.vector.holders.*;

import com.sun.codemodel.JType;
import com.sun.codemodel.JCodeModel;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.dremio.exec.vector.accessor.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;

import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;
import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;
/**
 * generated from ${.template_name}
 */
public class TypeHelper extends BasicTypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeHelper.class);

  public static SqlAccessor getSqlAccessor(ValueVector vector){
    final MinorType type = org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(vector.getField().getType());
    switch(type){
    case UNION:
      return new UnionSqlAccessor((UnionVector) vector);
    <#list vv.types as type>
    <#list type.minor as minor>
      <#assign typeMapping = TypeMappings[minor.class]!{}>
      <#assign supported = typeMapping.supported!true>
      <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
      <#if supported>
    case ${minor.class?upper_case}:
      return new Nullable${minor.class}Accessor((Nullable${minor.class}Vector) vector);
    </#if>  
    </#list>
    </#list>
    case MAP:
    case LIST:
      return new GenericAccessor(vector);
    }
    throw new UnsupportedOperationException(buildErrorMessage("find sql accessor", (type)));
  }
  
  public static JType getHolderType(JCodeModel model, com.dremio.common.types.TypeProtos.MinorType type, com.dremio.common.types.TypeProtos.DataMode mode){
    switch (type) {
    case UNION:
      return model._ref(UnionHolder.class);
    case MAP:
    case LIST:
      return model._ref(ComplexHolder.class);
      
<#list vv.types as type>
  <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
      <#assign supported = typeMapping.supported!true>
      <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
      <#if supported>
      case ${dremioMinorType}:
        switch (mode) {
          case REQUIRED:
            return model._ref(${minor.class}Holder.class);
          case OPTIONAL:
            return model._ref(Nullable${minor.class}Holder.class);
        }
    </#if>
  </#list>
</#list>
    case GENERIC_OBJECT:
      return model._ref(ObjectHolder.class);
      default:
        break;
      }
    throw new UnsupportedOperationException(buildErrorMessage("get holder type", com.dremio.common.util.MajorTypeHelper.getArrowMinorType(type)));
  }

  public static <T extends ValueVector> Class<T> getValueVectorClass(Field field) {
    return (Class<T>) getValueVectorClass(getMinorTypeForArrowType(field.getType()));
  }

  public static void load(ValueVector v, SerializedField metadata, ArrowBuf buffer) {
    if (v instanceof ZeroVector) {
      new ZeroVectorHelper((ZeroVector) v).load(metadata, buffer);
      return;
    } else if (v instanceof UnionVector) {
      new UnionVectorHelper((UnionVector) v).load(metadata, buffer);
      return;
    } else if (v instanceof ListVector) {
      new ListVectorHelper((ListVector) v).load(metadata, buffer);
      return;
    } else if (v instanceof NullableMapVector) {
      new NullableMapVectorHelper((NullableMapVector) v).load(metadata, buffer);
      return;
    } else if (v instanceof MapVector) {
      new MapVectorHelper((MapVector) v).load(metadata, buffer);
      return;
    } else
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
      <#assign supported = typeMapping.supported!true>
      <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
      <#if supported>
    if (v instanceof ${minor.class}Vector) {
        new ${minor.class}VectorHelper((${minor.class}Vector) v).load(metadata, buffer);
        return;
    } else if (v instanceof Nullable${minor.class}Vector) {
        new Nullable${minor.class}VectorHelper((Nullable${minor.class}Vector) v).load(metadata, buffer);
        return;
    }
    </#if>
    </#list>
    </#list>
    throw new UnsupportedOperationException(String.format("no loader for vector %s", v));
  }

  public static SerializedField.Builder getMetadataBuilder(ValueVector v) {
    if (v instanceof UnionVector) {
      return null;
    }
    if (v instanceof ListVector) {
      return new ListVectorHelper((ListVector) v).getMetadataBuilder();
    }
    if (v instanceof MapVector){
      return null;
    }
    if (v instanceof NullableMapVector){
      return null;
    }
    <#list vv.types as type>
    <#list type.minor as minor>
      <#assign typeMapping = TypeMappings[minor.class]!{}>
      <#assign supported = typeMapping.supported!true>
      <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
      <#if supported>
    if (v instanceof ${minor.class}Vector)
      return new ${minor.class}VectorHelper((${minor.class}Vector) v).getMetadataBuilder();
    if (v instanceof ${minor.class}Vector)
      return new Nullable${minor.class}VectorHelper((Nullable${minor.class}Vector) v).getMetadataBuilder();
    </#if>
    </#list>
    </#list>
    return null;
  }

  public static SerializedField getMetadata(ValueVector v) {
    if (v instanceof ZeroVector){
      return new ZeroVectorHelper((ZeroVector)v).getMetadata();
    }
    if (v instanceof UnionVector){
      return new UnionVectorHelper((UnionVector)v).getMetadata();
    }
    if (v instanceof ListVector){
      return new ListVectorHelper((ListVector)v).getMetadata();
    }
    if (v instanceof NullableMapVector){
      return new NullableMapVectorHelper((NullableMapVector)v).getMetadata();
    }
    if (v instanceof MapVector){
      return new MapVectorHelper((MapVector)v).getMetadata();
    }
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
      <#assign supported = typeMapping.supported!true>
      <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
    <#if supported>
    if (v instanceof ${minor.class}Vector)
    return new ${minor.class}VectorHelper((${minor.class}Vector) v).getMetadata();
    if (v instanceof Nullable${minor.class}Vector)
    return new Nullable${minor.class}VectorHelper((Nullable${minor.class}Vector) v).getMetadata();
    </#if>
    </#list>
    </#list>
    throw new UnsupportedOperationException(String.format("no metadata for vector %s", v));
  }

  public static ValueHolder getRequiredHolder(ValueHolder h) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    <#assign fields = minor.fields!type.fields />
    if (h instanceof Nullable${minor.class}Holder) {
      Nullable${minor.class}Holder holder = (Nullable${minor.class}Holder) h;
      ${minor.class}Holder ${minor.class}Holder = new ${minor.class}Holder();
    <#list fields as field>
      ${minor.class}Holder.${field.name} = holder.${field.name};
    </#list>
    <#if minor.typeParams??>
    <#list minor.typeParams as typeParam>
      ${minor.class}Holder.${typeParam.name} = holder.${typeParam.name};
    </#list>
    </#if>
      return ${minor.class}Holder;
    }
    </#if>
    </#list>
    </#list>
    return h;
  }

  public static Field getFieldForSerializedField(SerializedField serializedField) {
    String name = serializedField.getNamePart().getName();
    org.apache.arrow.vector.types.Types.MinorType arrowMinorType = getArrowMinorType(serializedField.getMajorType().getMinorType());
    switch(serializedField.getMajorType().getMinorType()) {
    case LIST:
      return new Field(name, true, arrowMinorType.getType(), ImmutableList.of(getFieldForSerializedField(serializedField.getChild(2))));
    case MAP: {
      ImmutableList.Builder<Field> builder = ImmutableList.builder();
      List<SerializedField> childList = serializedField.getChildList();
      Preconditions.checkState(childList.size() > 0, "children should start with $bits$ vector");
      SerializedField bits = childList.get(0);
      Preconditions.checkState(bits.getNamePart().getName().equals("$bits$"), "children should start with $bits$ vector: %s", childList);
      for (int i = 1; i < childList.size(); i++) {
        SerializedField child = childList.get(i);
        builder.add(getFieldForSerializedField(child));
      }
      return new Field(name, true, arrowMinorType.getType(), builder.build());
    }
    case UNION: {
      ImmutableList.Builder<Field> builder = ImmutableList.builder();
      final List<SerializedField> unionChilds = serializedField.getChild(1).getChildList();
      final int typeIds[] = new int[unionChilds.size()];
      for (int i=0; i < unionChilds.size(); i++) {
        final Field childField = getFieldForSerializedField(unionChilds.get(i));
        builder.add(childField);
        typeIds[i] =  Types.getMinorTypeForArrowType(childField.getType()).ordinal();
      }

      // TODO: not sure the sparse mode is correct.
      final Union unionType = new Union(UnionMode.Sparse, typeIds);
      return new Field(name, true, unionType, builder.build());
    }
    case DECIMAL:
      return new Field(name, true, new Decimal(serializedField.getMajorType().getPrecision(), serializedField.getMajorType().getScale()), null);
    default:
      return new Field(name, true, arrowMinorType.getType(), null);
    }
  }
}
