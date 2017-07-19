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
package com.dremio.exec.planner.physical.visitor;

import java.util.Collection;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;

import com.dremio.exec.planner.physical.DictionaryLookupPrel;
import com.dremio.exec.planner.physical.Prel;
import com.google.common.collect.Lists;

/**
 * Prel along with metadata of dictionary encoded fields.
 */
class PrelWithDictionaryInfo {
  private final Prel prel;
  private final GlobalDictionaryFieldInfo[] fields;

  PrelWithDictionaryInfo(Prel prel) {
    this.prel = prel;
    fields = new GlobalDictionaryFieldInfo[prel.getRowType().getFieldCount()];
  }

  PrelWithDictionaryInfo(Prel prel, GlobalDictionaryFieldInfo[] fields) {
    this.prel = prel;
    this.fields = fields.clone(); // make a copy
    assert prel.getRowType().getFieldCount() == fields.length;
  }

  Prel getPrel() {
    return prel;
  }

  boolean hasGlobalDictionary(int i) {
    return fields[i] != null;
  }

  void setGlobalDictionary(int i, GlobalDictionaryFieldInfo fieldInfo) {
    fields[i] = fieldInfo;
  }

  GlobalDictionaryFieldInfo getGlobalDictionaryFieldInfo(int i) {
    return fields[i];
  }

  public final GlobalDictionaryFieldInfo[] getFields() {
    return fields;
  }

  boolean hasDictionaryEncodedFields() {
    for (int i = 0; i < fields.length; ++i) {
      if (fields[i] != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Decode all fields that are global dictionary encoded.
   * It adds a dictionary lookup operations on top of current operation.
   * @return dictionary lookup operator who input is current prel.
   */
  DictionaryLookupPrel decodeAllFields() {
    final List<RelDataTypeField> newFieldList = Lists.newArrayList();
    final List<RelDataTypeField> oldFieldList = prel.getRowType().getFieldList();
    final List<GlobalDictionaryFieldInfo> fieldsToDecode = Lists.newArrayList();
    for (int i = 0; i < fields.length; ++i) {
      if (fields[i] != null) {
        final RelDataTypeField oldField = oldFieldList.get(i);
        fieldsToDecode.add(fields[i]);
        newFieldList.add(new RelDataTypeFieldImpl(oldField.getName(), oldField.getIndex(), fields[i].getRelDataTypeField().getType()));
      } else {
        newFieldList.add(oldFieldList.get(i));
      }
    }

    return new DictionaryLookupPrel(
      prel.getCluster(),
      prel.getTraitSet(),
      prel,
      toRowDataType(newFieldList, prel.getCluster().getTypeFactory()),
      fieldsToDecode);
  }


  static RelDataType toRowDataType(Collection<RelDataTypeField> fields, RelDataTypeFactory factory) {
    final RelDataTypeFactory.FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder(factory);
    for (RelDataTypeField field: fields) {
      builder.add(field);
    }
    return builder.build();
  }

  /**
   * Decode specified fields that are global dictionary encoded.
   * It adds a dictionary lookup operations on top of current operation.
   * @param decodeFieldIndices field indices to decode
   * @return If at least one field is being decoded then return dictionary lookup operator who input is current prel
   *         otherwise return current prel (this)
   */
  PrelWithDictionaryInfo decodeFields(Collection<Integer> decodeFieldIndices) {
    if (decodeFieldIndices.isEmpty()) {
      return this;
    }
    final List<RelDataTypeField> oldFieldList = prel.getRowType().getFieldList();
    final List<RelDataTypeField> newFieldList = Lists.newArrayList();
    final List<GlobalDictionaryFieldInfo> fieldsToDecode = Lists.newArrayList();
    final GlobalDictionaryFieldInfo[] newGlobalDictionaryFieldInfos = new GlobalDictionaryFieldInfo[fields.length];

    boolean decoded = false;
    for (int i = 0; i < fields.length; ++i) {
      if (fields[i] != null && decodeFieldIndices.contains(i)) {
        final RelDataTypeField oldField = oldFieldList.get(i);
        fieldsToDecode.add(fields[i]);
        newFieldList.add(new RelDataTypeFieldImpl(oldField.getName(), oldField.getIndex(), fields[i].getRelDataTypeField().getType()));
        newGlobalDictionaryFieldInfos[i] = null; // don't need to decode anymore
        decoded = true;
      } else {
        // copy through
        newGlobalDictionaryFieldInfos[i] = fields[i];
        newFieldList.add(oldFieldList.get(i));
      }
    }
    // None of the fields have global dictionary or they are already decoded, return current prel
    if (!decoded) {
      return this;
    }
    return new PrelWithDictionaryInfo(new DictionaryLookupPrel(
      prel.getCluster(),
      prel.getTraitSet(),
      prel,
      toRowDataType(newFieldList, prel.getCluster().getTypeFactory()),
      fieldsToDecode), newGlobalDictionaryFieldInfos);
  }
}
