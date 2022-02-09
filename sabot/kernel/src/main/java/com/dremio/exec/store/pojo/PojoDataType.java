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
package com.dremio.exec.store.pojo;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;

import com.dremio.exec.store.RecordDataType;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * This class uses reflection of a Java class to construct a {@link com.dremio.exec.store.RecordDataType}.
 */
public class PojoDataType extends RecordDataType {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PojoDataType.class);

  private final List<SqlTypeName> types = Lists.newArrayList();
  private final List<String> names = Lists.newArrayList();
  private final Class<?> pojoClass;

  public PojoDataType(Class<?> pojoClass) {
    this.pojoClass = pojoClass;
    for (Field f : pojoClass.getDeclaredFields()) {
      if (Modifier.isStatic(f.getModifiers())) {
        continue;
      }

      Class<?> type = f.getType();
      names.add(f.getName());

      if (type == int.class || type == Integer.class) {
        types.add(SqlTypeName.INTEGER);
      } else if(type == boolean.class || type == Boolean.class) {
        types.add(SqlTypeName.BOOLEAN);
      } else if(type == long.class || type == Long.class) {
        types.add(SqlTypeName.BIGINT);
      } else if(type == double.class || type == Double.class) {
        types.add(SqlTypeName.DOUBLE);
      } else if(type == String.class) {
        types.add(SqlTypeName.VARCHAR);
      } else if(type.isEnum()) {
        types.add(SqlTypeName.VARCHAR);
      } else if (type == Timestamp.class || type == DateTime.class) {
        types.add(SqlTypeName.TIMESTAMP);
      } else if (type == List.class || type == Collection.class) {
        types.add(SqlTypeName.ARRAY);
      } else {
        throw new RuntimeException(String.format("PojoDataType doesn't yet support conversions from type [%s].", type));
      }
    }
  }

  public Class<?> getPojoClass() {
    return pojoClass;
  }

  @Override
  public List<SqlTypeName> getFieldSqlTypeNames() {
    return types;
  }

  @Override
  public List<String> getFieldNames() {
    return names;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PojoDataType that = (PojoDataType) o;
    // Compare lists here since the column name/type ordering should be always the same
    return Objects.equal(types, that.types) &&
        Objects.equal(names, that.names) &&
        Objects.equal(pojoClass, that.pojoClass);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(types, names, pojoClass);
  }
}
