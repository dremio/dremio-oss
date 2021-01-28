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
package com.dremio.exec.store.parquet;

import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.List;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Struct;

import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TypeCoercion;

/**
 * Implements the TypeCoercion interface for Parquet reader
 */
public class ParquetTypeCoercion implements TypeCoercion {
  private final Map<String, Field> fieldsByName;

  public ParquetTypeCoercion(Map<String, Field> fieldsByName) {
    this.fieldsByName = fieldsByName;
  }

  @Override
  public TypeProtos.MajorType getType(Field field) {
    return MajorTypeHelper.getMajorTypeForField(field);
  }

  @Override
  public TypeCoercion getChildTypeCoercion(String fieldName, BatchSchema childSchema) {
    Map<String, Field> childrenByName = CaseInsensitiveMap.newHashMap();
    Field typeInfo = fieldsByName.get(fieldName);
    if (typeInfo.getType().getTypeID() == Struct || typeInfo.getType().getTypeID() == List) {
      for (Field field : childSchema.getFields()) {
        childrenByName.put(field.getName(), field);
      }
    }
    return new ParquetTypeCoercion(childrenByName);
  }
}
