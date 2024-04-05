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
package com.dremio.exec.record;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.arrow.vector.types.SerializedFieldHelper;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * A reusable builder that supports the creation of BatchSchemas. Can have a supporting expected
 * object. If the expected Schema object is defined, the builder will always check that this schema
 * is a equal or more materialized version of the current schema.
 */
public class SchemaBuilder {
  private List<Field> fields = Lists.newArrayList();

  private BatchSchema.SelectionVectorMode selectionVectorMode = SelectionVectorMode.NONE;

  SchemaBuilder() {}

  //  /**
  //   * Add a field where we don't have type information. In this case, DataType will be set to
  // LATEBIND and valueClass
  //   * will be set to null.
  //   *
  //   * @param fieldId
  //   *          The desired fieldId. Should be unique for this BatchSchema.
  //   * @param nullable
  //   *          Whether this field supports nullability.
  //   * @param mode
  //   * @throws SchemaChangeException
  //   */
  //  public void addLateBindField(short fieldId, boolean nullable, ValueMode mode) throws
  // SchemaChangeException {
  //    addTypedField(fieldId, DataType.LATEBIND, nullable, mode, Void.class);
  //  }

  public SchemaBuilder setSelectionVectorMode(BatchSchema.SelectionVectorMode selectionVectorMode) {
    this.selectionVectorMode = selectionVectorMode;
    return this;
  }

  public SchemaBuilder addFields(Iterable<Field> fields) {
    for (Field f : fields) {
      addField(f);
    }
    return this;
  }

  public SchemaBuilder addSerializedFields(Iterable<SerializedField> fields) {
    for (SerializedField f : fields) {
      addField(SerializedFieldHelper.create(f));
    }
    return this;
  }

  //  private void setTypedField(short fieldId, DataType type, boolean nullable, ValueMode mode,
  // Class<?> valueClass)
  //      throws SchemaChangeException {
  //    MaterializedField f = new MaterializedField(fieldId, type, nullable, mode, valueClass);
  //    if (expectedFields != null) {
  //      if (!expectedFields.containsKey(f.getFieldId()))
  //        throw new SchemaChangeException(
  //            String
  //                .format(
  //                    "You attempted to add a field for Id An attempt was made to add a duplicate
  // fieldId to the schema.  The offending fieldId was %d",
  //                    fieldId));
  //      f.checkMaterialization(expectedFields.lget()); // TODO: lget is not safe if expectedFields
  // is shared
  //    }
  //    fields.put(f.getFieldId(), f);
  //  }

  public SchemaBuilder addField(Field f) {
    fields.add(f);
    return this;
  }

  //  public void addTypedField(short fieldId, DataType type, boolean nullable, ValueMode mode,
  // Class<?> valueClass)
  //      throws SchemaChangeException {
  //    if (fields.containsKey(fieldId))
  //      throw new SchemaChangeException(String.format(
  //          "An attempt was made to add a duplicate fieldId to the schema.  The offending fieldId
  // was %d", fieldId));
  //    setTypedField(fieldId, type, nullable, mode, valueClass);
  //  }
  //
  //  public void replaceTypedField(short fieldId, DataType type, boolean nullable, ValueMode mode,
  // Class<?> valueClass)
  //      throws SchemaChangeException {
  //    if (!fields.containsKey(fieldId))
  //      throw new SchemaChangeException(
  //          String.format("An attempt was made to replace a field in the schema, however the
  // schema does " +
  //              "not currently contain that field id.  The offending fieldId was %d", fieldId));
  //    setTypedField(fieldId, type, nullable, mode, valueClass);
  //  }

  public SchemaBuilder removeField(Field f) throws SchemaChangeException {
    if (!fields.remove(f)) {
      throw new SchemaChangeException("You attempted to remove an nonexistent field.");
    }
    return this;
  }

  /**
   * Generate a new BatchSchema object based on the current state of the builder.
   *
   * @return
   * @throws SchemaChangeException
   */
  public BatchSchema build() {
    List<Field> fieldList = Lists.newArrayList(fields);
    return new BatchSchema(this.selectionVectorMode, fieldList);
  }
}
