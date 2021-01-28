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

package com.dremio.exec.store.hive.metadata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;

import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.exec.store.hive.exec.HiveDatasetOptions;
import com.dremio.exec.store.hive.exec.HiveReaderProtoUtil;
import com.dremio.exec.store.parquet.ManagedSchema;
import com.dremio.exec.store.parquet.ManagedSchemaField;
import com.dremio.hive.proto.HiveReaderProto;
import com.google.common.base.Splitter;

/**
 * Class for capturing hive schema
 */
public class ManagedHiveSchema implements ManagedSchema {

  private final Map<String, ManagedSchemaField> fieldInfo;
  private final Map<String, TypeInfo> typeInfo;
  private final boolean varcharTruncationEnabled;

  public ManagedHiveSchema(final JobConf jobConf, final HiveReaderProto.HiveTableXattr tableXattr) {
    final java.util.Properties tableProperties = new java.util.Properties();
    HiveUtilities.addProperties(jobConf, tableProperties, HiveReaderProtoUtil.getTableProperties(tableXattr));
    final String fieldNameProp = Optional.ofNullable(tableProperties.getProperty("columns")).orElse("");
    final String fieldTypeProp = Optional.ofNullable(tableProperties.getProperty("columns.types")).orElse("");
    varcharTruncationEnabled = HiveDatasetOptions
        .enforceVarcharWidth(HiveReaderProtoUtil.convertValuesToNonProtoAttributeValues(tableXattr.getDatasetOptionMap()));

    final Iterator<String> fieldNames = Splitter.on(",").trimResults().split(fieldNameProp).iterator();
    final Iterator<TypeInfo> fieldTypes = TypeInfoUtils.getTypeInfosFromTypeString(fieldTypeProp).iterator();

    final Map<String, ManagedSchemaField> schemaFieldMap = new HashMap<>();
    final Map<String, TypeInfo> typeInfoMap = new HashMap<>();
    while (fieldNames.hasNext() && fieldTypes.hasNext()) {
      final String fieldName = fieldNames.next();
      final TypeInfo fieldType = fieldTypes.next();
      fillSchemaFieldAndTypeInfoMap(fieldName, fieldType, schemaFieldMap, typeInfoMap);
    }
    fieldInfo = CaseInsensitiveMap.newImmutableMap(schemaFieldMap);
    typeInfo = CaseInsensitiveMap.newImmutableMap(typeInfoMap);
  }

  private void fillSchemaFieldAndTypeInfoMap(String fieldName, TypeInfo fieldType, Map<String, ManagedSchemaField> schemaFieldMap, Map<String, TypeInfo> typeInfoMap) {
    ManagedSchemaField field;
    if (fieldType instanceof DecimalTypeInfo) {
      field = ManagedSchemaField.newFixedLenField(fieldName, fieldType.getTypeName(),
        ((DecimalTypeInfo) fieldType).getPrecision(), ((DecimalTypeInfo) fieldType).getScale());
      typeInfoMap.put(fieldName, fieldType);
      schemaFieldMap.put(fieldName, field);
    } else if (fieldType instanceof BaseCharTypeInfo) {
      if (varcharTruncationEnabled) {
        field = ManagedSchemaField.newFixedLenField(fieldName, fieldType.getTypeName(),
          ((BaseCharTypeInfo) fieldType).getLength(), 0);
        typeInfoMap.put(fieldName, fieldType);
      } else {
        field = ManagedSchemaField.newUnboundedLenField(fieldName, fieldType.getTypeName());
      }
      schemaFieldMap.put(fieldName, field);
    } else if (fieldType instanceof StructTypeInfo) {
      Iterator<String> structFieldNames = ((StructTypeInfo) fieldType).getAllStructFieldNames().iterator();
      Iterator<TypeInfo> structTypeInfos = ((StructTypeInfo) fieldType).getAllStructFieldTypeInfos().iterator();
      while (structFieldNames.hasNext() && structTypeInfos.hasNext()) {
        fillSchemaFieldAndTypeInfoMap(fieldName + "." + structFieldNames.next(), structTypeInfos.next(), schemaFieldMap, typeInfoMap);
      }
      field = ManagedSchemaField.newUnboundedLenField(fieldName, fieldType.getTypeName());
      typeInfoMap.put(fieldName, fieldType);
      schemaFieldMap.put(fieldName, field);
    } else {
      // Extend ManagedSchemaField.java in case granular information has to be stored.
      // No mention of len and scale means it is unbounded. So, we store max values.
      field = ManagedSchemaField.newUnboundedLenField(fieldName, fieldType.getTypeName());
      typeInfoMap.put(fieldName, fieldType);
      schemaFieldMap.put(fieldName, field);
    }
  }

  @Override
  public Optional<ManagedSchemaField> getField(final String fieldName) {
    return Optional.ofNullable(fieldInfo.get(fieldName));
  }

  public Map<String, ManagedSchemaField> getAllFields() {
    return this.fieldInfo;
  }

  @Override
  public boolean isVarcharTruncationEnabled() {
    return varcharTruncationEnabled;
  }

  public Map<String, TypeInfo> getTypeInfos() {
    return this.typeInfo;
  }

  @Override
  public String toString() {
    return "HiveSchema{" +
      "fieldInfo=" + fieldInfo +
      '}';
  }
}
