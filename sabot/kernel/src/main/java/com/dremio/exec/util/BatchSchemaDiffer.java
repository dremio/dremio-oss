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
package com.dremio.exec.util;

import com.dremio.common.exceptions.UserException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Given 2 field lists will compute a deep difference between them and get list of dropped, updated
 * and added columns.
 */
public class BatchSchemaDiffer {
  private final Field parent;

  private boolean areComplexToPrimitiveConversionsAllowed;

  private BatchSchemaDiffer(Field parent, boolean areComplexToPrimitiveConversionsAllowed) {
    this.parent = parent;
    this.areComplexToPrimitiveConversionsAllowed = areComplexToPrimitiveConversionsAllowed;
  }

  public BatchSchemaDiffer() {
    this.parent = null;
  }

  public static BatchSchemaDiffer withParent(
      Field parent, boolean areComplexToPrimitiveConversionsAllowed) {
    return new BatchSchemaDiffer(parent, areComplexToPrimitiveConversionsAllowed);
  }

  public void setAllowComplexToPrimitiveConversions(
      boolean areComplexToPrimitiveConversionsAllowed) {
    this.areComplexToPrimitiveConversionsAllowed = areComplexToPrimitiveConversionsAllowed;
  }

  public BatchSchemaDiff diff(List<Field> oldFields, List<Field> newFields) {
    // (oldFields-newFields) gives fields deleted in newFields
    // for each deleted:
    //    deleted.stream().map(d -> {return appendParent ? new parent with d as child :
    // d}).forEach(diff::droppedField);

    // (newFields-oldFields) gives field added in newFields
    // for each added:
    //    added.stream().map(d -> { return appendParent ? new parent with d as child :
    // d}).forEach(diff::addedField);

    // for common fields:
    // if field is primitive
    // move on if type is same else add to diff.modified with or without parent depending on
    // appendParent

    // if complex do
    // BatchSchemaDiffer.withParent(complexField.topLevel).diff(leftComplex.getChildren(),
    // newFields.getChildren())
    // and add all fields to diff.modified with parent if appendParent is true

    BatchSchemaDiff diff = new BatchSchemaDiff();

    Map<String, Field> oldFieldMap = new LinkedHashMap<>();
    Map<String, Field> newFieldMap = new LinkedHashMap<>();

    for (Field field : oldFields) {
      oldFieldMap.put(field.getName().toLowerCase(), field);
    }

    for (Field field : newFields) {
      newFieldMap.put(field.getName().toLowerCase(), field);
    }

    List<Field> deletedFields =
        Maps.difference(oldFieldMap, newFieldMap).entriesOnlyOnLeft().values().stream()
            .collect(Collectors.toList());
    deletedFields =
        deletedFields.stream()
            .flatMap(this::getFlattenedChildFieldsFromField)
            .collect(Collectors.toList());

    diff.droppedField(deletedFields);

    List<Field> addedFields =
        Maps.difference(oldFieldMap, newFieldMap).entriesOnlyOnRight().values().stream()
            .collect(Collectors.toList());

    addedFields =
        addedFields.stream()
            .flatMap(this::getFlattenedChildFieldsFromField)
            .collect(Collectors.toList());

    diff.addedField(addedFields);

    // any 2 fields with the same name are common fields. Type may have
    // changed.
    newFieldMap.keySet().retainAll(oldFieldMap.keySet());
    List<Field> commonFields = newFieldMap.values().stream().collect(Collectors.toList());

    for (Field f : commonFields) {
      if (isNotAValidTypeConversion(oldFieldMap, f)) {
        throw UserException.invalidMetadataError()
            .message(
                String.format(
                    "Field %s and %s are incompatible types, for type changes please ensure both columns are either of primitive types or complex but not mixed.",
                    oldFieldMap.get(f.getName().toLowerCase()), f.getType()))
            .buildSilently();
      }
      boolean isSimple = !f.getType().isComplex();
      // Simple field with types not same.
      if (isSimple) {
        if (!oldFieldMap.get(f.getName().toLowerCase()).getType().equals(f.getType())) {
          diff.modifiedField(appendParent(parent, f));
        }
      } else {
        // Is a complex field
        BatchSchemaDiff batchSchemaDiff =
            BatchSchemaDiffer.withParent(f, areComplexToPrimitiveConversionsAllowed)
                .diff(oldFieldMap.get(f.getName().toLowerCase()).getChildren(), f.getChildren());
        diff.addedField(
            batchSchemaDiff.getAddedFields().stream()
                .flatMap(this::getFlattenedChildFieldsFromField)
                .collect(Collectors.toList()));

        diff.droppedField(
            batchSchemaDiff.getDroppedFields().stream()
                .flatMap(this::getFlattenedChildFieldsFromField)
                .collect(Collectors.toList()));

        diff.modifiedField(
            batchSchemaDiff.getModifiedFields().stream()
                .flatMap(this::getFlattenedChildFieldsFromField)
                .collect(Collectors.toList()));
      }
    }
    return diff;
  }

  private boolean isNotAValidTypeConversion(Map<String, Field> oldFieldMap, Field f) {
    return (!areComplexToPrimitiveConversionsAllowed
            && f.getType().isComplex()
                ^ oldFieldMap.get(f.getName().toLowerCase()).getType().isComplex())
        || (areComplexToPrimitiveConversionsAllowed
            && f.getType().isComplex()
            && !oldFieldMap.get(f.getName().toLowerCase()).getType().isComplex());
  }

  private static List<Field> appendParent(Field parent, Field child) {
    if (parent == null) {
      // return the child if there is no parent
      return ImmutableList.of(child);
    }
    return ImmutableList.of(
        new Field(parent.getName(), parent.getFieldType(), ImmutableList.of(child)));
  }

  private Stream<Field> getFlattenedChildFieldsFromField(Field field) {
    if (this.parent != null) {
      field = new Field(parent.getName(), parent.getFieldType(), ImmutableList.of(field));
    }
    return appendChildren(field);
  }

  private static Stream<Field> appendChildren(Field field) {
    if (!field.getType().isComplex()) {
      return Stream.of(field);
    }
    Stream<Field> result = Stream.empty();
    for (Field child : field.getChildren()) {
      result =
          Stream.concat(
              result,
              appendChildren(child)
                  .map(f -> new Field(field.getName(), field.getFieldType(), ImmutableList.of(f))));
    }
    return result;
  }
}
