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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.store.CoercionReader;
import com.dremio.exec.store.TypeCoercion;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.hive.proto.HiveReaderProto;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implements the TypeCoercion interface for Hive Parquet reader
 */
public class HiveTypeCoercion implements TypeCoercion {

  private Map<String, Integer> varcharWidthMap;

  HiveTypeCoercion(JobConf jobConf, HiveReaderProto.HiveTableXattr tableXattr) {
    final java.util.Properties tableProperties = new java.util.Properties();
    HiveUtilities.addProperties(jobConf, tableProperties, HiveReaderProtoUtil.getTableProperties(tableXattr));
    final String columnNameProperty = tableProperties.getProperty("columns");
    final String columnTypeProperty = tableProperties.getProperty("columns.types");

    final List<String> columnNames;
    final List<TypeInfo> columnTypes;
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    Preconditions.checkArgument(columnNames.size() == columnTypes.size(),
      "columnNames and columnTypes have different sizes");

    Iterator columnTypeIterator = columnTypes.iterator();
    Iterator columnNameIterator = columnNames.iterator();

    varcharWidthMap = new HashMap<>();

    while(columnNameIterator.hasNext() && columnTypeIterator.hasNext()) {
      String name = (String) columnNameIterator.next();
      TypeInfo typeInfo = (TypeInfo) columnTypeIterator.next();
      if (typeInfo instanceof VarcharTypeInfo) {
        varcharWidthMap.put(name, (Integer) ((VarcharTypeInfo) typeInfo).getLength());
      } else if (typeInfo instanceof CharTypeInfo) {
        varcharWidthMap.put(name, (Integer) ((CharTypeInfo) typeInfo).getLength());
      }
    }
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
