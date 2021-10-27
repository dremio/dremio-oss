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
package com.dremio.exec.store.hive.exec;

import java.util.Collections;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TypeCoercion;

/**
 * Implements the TypeCoercion interface for Hive Parquet reader
 */
public class HiveTypeCoercion implements TypeCoercion {
  private final Map<String, TypeInfo> typeInfoMap;
  private Map<String, Integer> varcharWidthMap = CaseInsensitiveMap.newHashMap();
  private final boolean varcharTruncationEnabled;

  HiveTypeCoercion(Map<String, TypeInfo> typeInfoMap, boolean varcharTruncationEnabled) {
    this.varcharTruncationEnabled = varcharTruncationEnabled;
    this.typeInfoMap = CaseInsensitiveMap.newImmutableMap(typeInfoMap);
    if (varcharTruncationEnabled) {
      typeInfoMap.entrySet().stream()
        .filter(e -> e.getValue() instanceof BaseCharTypeInfo)
        .forEach(e -> varcharWidthMap.put(e.getKey(), ((BaseCharTypeInfo) e.getValue()).getLength()));
      varcharWidthMap = Collections.unmodifiableMap(varcharWidthMap);
    }
  }

  @Override
  public MajorType getType(Field field) {
    MajorType majorType = MajorTypeHelper.getMajorTypeForField(field);
    if (majorType.getMinorType().equals(MinorType.VARCHAR) ||
      majorType.getMinorType().equals(MinorType.VARBINARY)) {
      int width = varcharWidthMap.getOrDefault(field.getName(), CompleteType.DEFAULT_VARCHAR_PRECISION);
      majorType = majorType.toBuilder().setWidth(width).build();
    }
    return majorType;
  }

  @Override
  public TypeCoercion getChildTypeCoercion(String fieldName, BatchSchema childSchema) {
    Map<String, TypeInfo> childrenTypeInfoMap = CaseInsensitiveMap.newHashMap();

    TypeInfo typeInfo = typeInfoMap.get(fieldName);
    if (typeInfo instanceof StructTypeInfo) {
      for (Field field: childSchema.getFields()) {
        childrenTypeInfoMap.put(field.getName(), ((StructTypeInfo) typeInfo).getStructFieldTypeInfo(field.getName()));
      }
    } else if (typeInfo instanceof ListTypeInfo) {
      childrenTypeInfoMap.put(childSchema.getFields().get(0).getName(), ((ListTypeInfo) typeInfo).getListElementTypeInfo());
    }
    return new HiveTypeCoercion(childrenTypeInfoMap, varcharTruncationEnabled);
  }

  @Override
  public boolean isVarcharTruncationRequired(Field field) {
    MajorType majorType = getType(field);
    MinorType minorType = majorType.getMinorType();
    return (minorType.equals(MinorType.VARCHAR) || minorType.equals(MinorType.VARBINARY)) &&
      majorType.getWidth() < CompleteType.DEFAULT_VARCHAR_PRECISION;
  }
}
