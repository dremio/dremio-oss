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
package com.dremio.service.autocomplete.catalog.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

/**
 * A record type that add fields based on requests.
 */
public class ExpandingRecordType extends DynamicRecordType {

  private final RelDataTypeFactory typeFactory;
  private final List<RelDataTypeField> fields = new ArrayList<>();

  public ExpandingRecordType(RelDataTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
    computeDigest();
  }

  @Override
  public List<RelDataTypeField> getFieldList() {
    addStarIfEmpty();
    return fields;
  }

  @Override
  public int getFieldCount() {
    addStarIfEmpty();
    return fields.size();
  }

  private void addStarIfEmpty() {
    if (fields.isEmpty()) {
      getField("**", false, false);
    }
  }

  @Override
  public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {

    /* First check if this field name exists in our field list */
    for (RelDataTypeField f : fields) {
      if (Util.matches(caseSensitive, f.getName(), fieldName)) {
        return f;
      }
    }

    /* This field does not exist in our field list add it */
    final SqlTypeName typeName = DynamicRecordType.isDynamicStarColName(fieldName)
        ? SqlTypeName.DYNAMIC_STAR : SqlTypeName.ANY;

    // This field does not exist in our field list add it
    RelDataTypeField newField = new RelDataTypeFieldImpl(
        fieldName,
        fields.size(),
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(typeName), true));

    /* Add the name to our list of field names */
    fields.add(newField);

    return newField;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
      return SqlTypeName.ANY;
  }

  @Override
  public RelDataTypePrecedenceList getPrecedenceList() {
    return new SqlTypeExplicitPrecedenceList(Collections.<SqlTypeName>emptyList());
  }

  @Override
  public List<String> getFieldNames() {
    return fields.stream().map(f -> f.getName()).collect(Collectors.toList());
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("(ExpandingRecordType" + getFieldNames() + ")");
  }

  @Override
  public boolean isStruct() {
    return true;
  }

  @Override
  public int hashCode() {
    return fields.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public RelDataTypeFamily getFamily() {
    return getSqlTypeName().getFamily();
  }
}
