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
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.store.CoercionReader;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.exec.store.parquet.ManagedSchema;
import com.dremio.exec.store.parquet.ManagedSchemaField;
import com.google.common.collect.Sets;

/**
 * Implements the TypeCoercion interface for Hive Parquet reader
 */
public class HiveTypeCoercion implements TypeCoercion {
  private Map<String, Integer> varcharWidthMap = CaseInsensitiveMap.newHashMap();

  HiveTypeCoercion(final ManagedSchema schema) {
    schema.getAllFields().entrySet().stream()
      .map(Map.Entry::getValue)
      .filter(ManagedSchemaField::isTextField)
      .forEach(e -> varcharWidthMap.put(e.getName(), e.getLength()));
    varcharWidthMap = Collections.unmodifiableMap(varcharWidthMap);
  }

  @Override
  public TypeProtos.MajorType getType(Field field, EnumSet<CoercionReader.Options> options) {
    TypeProtos.MajorType majorType = MajorTypeHelper.getMajorTypeForField(field);
    if (options.contains(CoercionReader.Options.SET_VARCHAR_WIDTH) && (majorType.getMinorType().equals(TypeProtos.MinorType
        .VARCHAR) ||
        majorType.getMinorType().equals(TypeProtos.MinorType.VARBINARY))) {
      int width = varcharWidthMap.getOrDefault(field.getName(), CompleteType.DEFAULT_VARCHAR_PRECISION);
      majorType = majorType.toBuilder().setWidth(width).build();
    }
    return majorType;
  }
}
