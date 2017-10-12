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
<@pp.changeOutputFile name="/org/apache/arrow/vector/util/BasicTypeHelper.java" />

<#include "/@includes/license.ftl" />

        package org.apache.arrow.vector.util;

<#include "/@includes/vv_imports.ftl" />
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.complex.impl.*;
import org.apache.arrow.vector.complex.writer.BaseWriter.*;
import org.apache.arrow.vector.complex.writer.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.util.ObjectType;

import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;
import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;
import static com.dremio.common.util.MajorTypeHelper.getMajorTypeForField;

/**
 * generated from ${.template_name}
 */
public class BasicTypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicTypeHelper.class);

  private static final int WIDTH_ESTIMATE = 50;

  // Default length when casting to varchar : 65536 = 2^16
  // This only defines an absolute maximum for values, setting
  // a high value like this will not inflate the size for small values
  public static final int VARCHAR_DEFAULT_CAST_LEN = 65536;

  protected static String buildErrorMessage(final String operation, final MinorType type) {
    return String.format("Unable to %s for minor type [%s]", operation, type);
  }

  public static int getSize(MinorType type) {
    switch (type) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case}:
    return ${type.width}<#if minor.class?substring(0, 3) == "Var" ||
            minor.class?substring(0, 3) == "PRO" ||
            minor.class?substring(0, 3) == "MSG"> + WIDTH_ESTIMATE</#if>;
    </#if>
    </#list>
    </#list>
    }
    throw new UnsupportedOperationException(buildErrorMessage("get size", type));
  }

  public static Class<?> getValueVectorClass(MinorType type){
    switch (type) {
    case UNION:
      return UnionVector.class;
    case MAP:
        return NullableMapVector.class;
    case LIST:
        return ListVector.class;
    case NULL:
        return ZeroVector.class;
      <#list vv.types as type>
      <#list type.minor as minor>
          <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case}:
      return Nullable${minor.class}Vector.class;
    </#if>  
    </#list>
    </#list>
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get value vector class", type));
  }
  /*
  public static Class<?> getReaderClassName(MinorType type){
    switch (type) {
    case MAP:
          return SingleMapReaderImpl.class;
    case LIST:
        return SingleListReaderImpl.class;
      <#list vv.types as type>
      <#list type.minor as minor>
          <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case}:
      return Nullable${minor.class}ReaderImpl.class;
    </#if>
    </#list>
    </#list>
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get reader class name", type, mode));
  }
  */

  public static Class<?> getWriterInterface(MinorType type){
    switch (type) {
    case UNION: return UnionWriter.class;
    case MAP: return MapWriter.class;
    case LIST: return ListWriter.class;
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case}: return ${minor.class}Writer.class;
    </#if>
    </#list>
    </#list>
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get writer interface", type));
  }

  public static Class<?> getWriterImpl( MinorType type ) {
    switch (type) {
    case UNION:
      return UnionWriter.class;
    case MAP:
        return NullableMapWriter.class;
    case LIST:
        return UnionListWriter.class;
      <#list vv.types as type>
      <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case}:
      return ${minor.class}WriterImpl.class;
    </#if>  
    </#list>
    </#list>
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get writer implementation", type));
  }

  public static Class<?> getHolderReaderImpl(MinorType type){
    switch (type) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case}:
      return Nullable${minor.class}HolderReaderImpl.class;
    </#if>
    </#list>
    </#list>
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get holder reader implementation", type));
  }

  public static FieldVector getNewVector(Field field, BufferAllocator allocator){
    return getNewVector(field, allocator, null);
  }
  public static FieldVector getNewVector(Field field, BufferAllocator allocator, CallBack callBack){
    if (field.getType() instanceof ObjectType) {
      return new ObjectVector(field.getName(), allocator);
    }

    MinorType type = org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(field.getType());

    List<Field> children = field.getChildren();
    
    switch (type) {

    case UNION:
      UnionVector unionVector = new UnionVector(field.getName(), allocator, callBack);
      if(!children.isEmpty()){
        unionVector.initializeChildrenFromFields(children);
      }
      return unionVector;
    case LIST:
      ListVector listVector = new ListVector(field.getName(), allocator, callBack);
      if(!children.isEmpty()){
        listVector.initializeChildrenFromFields(children);
      }
      return listVector;
    case MAP:
      NullableMapVector mapVector = new NullableMapVector(field.getName(), allocator, callBack);
      if(!children.isEmpty()){
        mapVector.initializeChildrenFromFields(children);
      }
      return mapVector;

    case NULL:
        return new ZeroVector();
      <#list vv.  types as type>
      <#list type.minor as minor>
      <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case}:
      <#if minor.class == "Decimal">
      Decimal dec = ((Decimal) field.getType());
      return new NullableDecimalVector(field.getName(), allocator, dec.getPrecision(), dec.getScale());
      <#else>
      return new Nullable${minor.class}Vector(field.getName(), allocator);
      </#if>
    </#if>  
    </#list>
    </#list>
    default:
      break;
    }
    // All ValueVector types have been handled.
    throw new UnsupportedOperationException(buildErrorMessage("get new vector", type));
  }

  public static com.dremio.common.types.TypeProtos.MajorType getValueHolderMajorType(Class<? extends ValueHolder> holderClass) {

    if (0 == 1) {
      return null;
    }
    <#list vv.types as type>
    <#list type.minor as minor>
      <#assign typeMapping = TypeMappings[minor.class]!{}>
      <#assign supported = typeMapping.supported!true>
      <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
      <#if ! supported>
      // unsupported type ${minor.class}
      <#else>
    else if (holderClass.equals(${minor.class}Holder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.${dremioMinorType});
    }
    else if (holderClass.equals(Nullable${minor.class}Holder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.${dremioMinorType});
    }
      </#if>
    </#list>
    </#list>
    else if (holderClass.equals(ObjectHolder.class)) {
      return com.dremio.common.types.Types.required(com.dremio.common.types.TypeProtos.MinorType.GENERIC_OBJECT);
    }
    else if (holderClass.equals(UnionHolder.class)) {
      return com.dremio.common.types.Types.optional(com.dremio.common.types.TypeProtos.MinorType.UNION);
    }

    throw new UnsupportedOperationException(String.format("%s is not supported for 'getValueHolderType' method.", holderClass.getName()));

  }

  public static ValueHolder getValueHolderForType(MinorType type) {

    switch(type) {
    <#list vv.types as type >
    <#list type.minor as minor >
        <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
            case ${minor.class?upper_case}:
              return new Nullable${minor.class}Holder();
    </#if>          
    </#list >
    </#list >
    }
    throw new UnsupportedOperationException(String.format("%s is not supported for 'getValueHolderForType' method.", type));

  }

  public static MinorType getValueHolderType(ValueHolder holder) {

    if (0 == 1) {
      return null;
    }
    <#list vv.types as type>
    <#list type.minor as minor>
        <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    else if (holder instanceof ${minor.class}Holder) {
      return MinorType.${minor.class?upper_case};
    }
    else if (holder instanceof Nullable${minor.class}Holder) {
      return MinorType.${minor.class?upper_case};
    }
    </#if>
    </#list>
    </#list>

    throw new UnsupportedOperationException("ValueHolder is not supported for 'getValueHolderType' method.");

  }

  public static void setNotNull(ValueHolder holder) {
    MinorType type = getValueHolderType(holder);

    switch(type) {
    <#list vv.types as type>
    <#list type.minor as minor>
        <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case} :
    if (holder instanceof Nullable${minor.class}Holder) {
      ((Nullable${minor.class}Holder) holder).isSet = 1;
      return;
    }
    </#if>
    </#list>
    </#list>
    default:
      throw new UnsupportedOperationException(buildErrorMessage("set not null", type));
    }
  }

  public static boolean isNull(ValueHolder holder) {
    MinorType type = getValueHolderType(holder);

    switch(type) {
    <#list vv.types as type>
    <#list type.minor as minor>
        <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case} :
    if (holder instanceof ${minor.class}Holder) {
      return false;
    } else{
      return ((Nullable${minor.class}Holder)holder).isSet == 0;
    }
    </#if>
    </#list>
    </#list>
    default:
      throw new UnsupportedOperationException(buildErrorMessage("check is null", type));
    }
  }

  public static void setValueSafe(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = getMajorTypeForField(vector.getField());

    switch(type.getMinorType()) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
    <#if ! supported>
    // unsupported type ${minor.class}
    <#else>
    case ${dremioMinorType} :
    switch (type.getMode()) {
    case REQUIRED:
      ((${minor.class}Vector) vector).getMutator().setSafe(index, (${minor.class}Holder) holder);
    return;
    case OPTIONAL:
    if (holder instanceof Nullable${minor.class}Holder) {
        if (((Nullable${minor.class}Holder) holder).isSet == 1) {
        ((Nullable${minor.class}Vector) vector).getMutator().setSafe(index, (Nullable${minor.class}Holder) holder);
      } else {
        ((Nullable${minor.class}Vector) vector).getMutator().isSafe(index);
      }
    } else {
      ((Nullable${minor.class}Vector) vector).getMutator().setSafe(index, (${minor.class}Holder) holder);
    }
    return;
    }
    </#if>
    </#list>
    </#list>
    case GENERIC_OBJECT:
      ((ObjectVector) vector).getMutator().setSafe(index, (ObjectHolder) holder);
    default:
      throw new UnsupportedOperationException(buildErrorMessage("set value safe", getArrowMinorType(type.getMinorType())));
    }
  }
 /*
  public static void setValueSafe(ValueVector vector, int index, ValueHolder holder) {
    MinorType type = getMinorTypeForArrowType(vector.getField().getType());

    switch(type) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case} :
      if (((Nullable${minor.class}Holder) holder).isSet == 1) {
        ((Nullable${minor.class}Vector) vector).getMutator().setSafe(index, (Nullable${minor.class}Holder) holder);
      }
    </#if>
    </#list>
    </#list>
    default:
      throw new UnsupportedOperationException(buildErrorMessage("set value safe", type));
    }
  }
  */

  public static ValueHolder createValueHolder(MinorType type) {
    switch(type) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case} :
      return new Nullable${minor.class}Holder();
    </#if>  
    </#list>
    </#list>
    default:
      throw new UnsupportedOperationException(buildErrorMessage("create value holder", type));
    }
  }

  public static ValueHolder getValue(ValueVector vector, int index) {
    MinorType type = getMinorTypeForArrowType(vector.getField().getType());
    ValueHolder holder;
    switch(type) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case} :
    <#if minor.class?starts_with("Var") || minor.class == "IntervalDay" || minor.class == "Interval" ||
            minor.class?starts_with("Decimal")>
      holder = new Nullable${minor.class}Holder();
    ((Nullable${minor.class}Holder)holder).isSet = ((Nullable${minor.class}Vector) vector).getAccessor().isSet(index);
    if (((Nullable${minor.class}Holder)holder).isSet == 1) {
      ((Nullable${minor.class}Vector) vector).getAccessor().get(index, (Nullable${minor.class}Holder)holder);
    }
    return holder;
    <#else>
      holder = new Nullable${minor.class}Holder();
    ((Nullable${minor.class}Holder)holder).isSet = ((Nullable${minor.class}Vector) vector).getAccessor().isSet(index);
    if (((Nullable${minor.class}Holder)holder).isSet == 1) {
      ((Nullable${minor.class}Holder)holder).value = ((Nullable${minor.class}Vector) vector).getAccessor().get(index);
    }
    return holder;
    </#if>
    </#if>
    </#list>
    </#list>
    }

    throw new UnsupportedOperationException(buildErrorMessage("get value", type));
  }

  public static ValueHolder deNullify(ValueHolder holder) {
    MajorType type = getValueHolderMajorType(holder.getClass());

    switch(type.getMinorType()) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
    <#if ! supported>
    // unsupported type ${minor.class}
    <#else>
    case ${dremioMinorType} :

    switch (type.getMode()) {
    case REQUIRED:
      return holder;
    case OPTIONAL:
      if( ((Nullable${minor.class}Holder) holder).isSet == 1) {
      ${minor.class}Holder newHolder = new ${minor.class}Holder();

      <#assign fields = minor.fields!type.fields />
      <#list fields as field>
      newHolder.${field.name} = ((Nullable${minor.class}Holder) holder).${field.name};
      </#list>
      <#if minor.typeParams??>
      <#list minor.typeParams as typeParam>
      newHolder.${typeParam.name} = ((Nullable${minor.class}Holder) holder).${typeParam.name};
      </#list>
      </#if>

      return newHolder;
    } else {
      throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
    }
    case REPEATED:
      return holder;
    }
    </#if>
    </#list>
    </#list>
    default:
      throw new UnsupportedOperationException(buildErrorMessage("deNullify", getArrowMinorType(type.getMinorType())));
    }
  }

  public static ValueHolder nullify(ValueHolder holder) {
    MajorType type = getValueHolderMajorType(holder.getClass());

    switch(type.getMinorType()) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#assign dremioMinorType = typeMapping.minor_type!minor.class?upper_case>
    <#if ! supported>
    // unsupported type ${minor.class}
    <#else>
    case ${dremioMinorType} :
    switch (type.getMode()) {
    case REQUIRED:
      Nullable${minor.class}Holder newHolder = new Nullable${minor.class}Holder();
    newHolder.isSet = 1;
    <#assign fields = minor.fields!type.fields />
    <#list fields as field>
    newHolder.${field.name} = ((${minor.class}Holder) holder).${field.name};
    </#list>
    return newHolder;
    case OPTIONAL:
      return holder;
    case REPEATED:
      throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
    }
    </#if>
    </#list>
    </#list>
    default:
      throw new UnsupportedOperationException(buildErrorMessage("nullify", getArrowMinorType(type.getMinorType())));
    }
  }



  /*
  public static void setValue(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = vector.getField().getType();

    switch(type.getMinorType()) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case} :
    switch (type.getMode()) {
    case REQUIRED:
      ((${minor.class}Vector) vector).getMutator().setSafe(index, (${minor.class}Holder) holder);
    return;
    case OPTIONAL:
      if (((Nullable${minor.class}Holder) holder).isSet == 1) {
      ((Nullable${minor.class}Vector) vector).getMutator().setSafe(index, (Nullable${minor.class}Holder) holder);
    }
    return;
    }
    </#if>
    </#list>
    </#list>
    case GENERIC_OBJECT:
      ((ObjectVector) vector).getMutator().setSafe(index, (ObjectHolder) holder);
      return;
    default:
      throw new UnsupportedOperationException(buildErrorMessage("set value", type));
    }
  }

  public static void setValueSafe(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = vector.getField().getType();

    switch(type.getMinorType()) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case} :
    switch (type.getMode()) {
    case REQUIRED:
      ((${minor.class}Vector) vector).getMutator().setSafe(index, (${minor.class}Holder) holder);
    return;
    case OPTIONAL:
      if (((Nullable${minor.class}Holder) holder).isSet == 1) {
      ((Nullable${minor.class}Vector) vector).getMutator().setSafe(index, (Nullable${minor.class}Holder) holder);
    } else {
      ((Nullable${minor.class}Vector) vector).getMutator().isSafe(index);
    }
    return;
    }
    </#if>
    </#list>
    </#list>
    case GENERIC_OBJECT:
      ((ObjectVector) vector).getMutator().setSafe(index, (ObjectHolder) holder);
    default:
      throw new UnsupportedOperationException(buildErrorMessage("set value safe", type));
    }
  }

  public static boolean compareValues(ValueVector v1, int v1index, ValueVector v2, int v2index) {
    MajorType type1 = v1.getField().getType();
    MajorType type2 = v2.getField().getType();

    if (type1.getMinorType() != type2.getMinorType()) {
      return false;
    }

    switch(type1.getMinorType()) {
    <#list vv.types as type>
    <#list type.minor as minor>
    <#assign typeMapping = TypeMappings[minor.class]!{}>
    <#assign supported = typeMapping.supported!true>
    <#if supported>
    case ${minor.class?upper_case} :
    if ( ((${minor.class}Vector) v1).getAccessor().get(v1index) ==
            ((${minor.class}Vector) v2).getAccessor().get(v2index) )
    return true;
    break;
    </#if>
    </#list>
    </#list>
    default:
      break;
    }
    return false;
  }

*/
}
