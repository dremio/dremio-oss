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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Given 2 field lists will compute a deep difference between them
 * and get list of dropped, updated and added columns.
 */
public class BatchSchemaDiffer {
  private final Field parent;

  public BatchSchemaDiffer(Field parent) {
    this.parent = parent;
  }

  public BatchSchemaDiffer() {
    this.parent = null;
  }

  public static BatchSchemaDiffer withParent(Field parent) {
    return new BatchSchemaDiffer(parent);
  }

  public BatchSchemaDiff diff(List<Field> left, List<Field> right) {
    // (left-right) gives fields deleted in right
    // for each deleted:
    //    deleted.stream().map(d -> {return appendParent ? new parent with d as child : d}).forEach(diff::droppedField);

    // (right-left) gives field added in right
    // for each added:
    //    added.stream().map(d -> { return appendParent ? new parent with d as child : d}).forEach(diff::addedField);

    // for common fields:
    // if field is primitive
    // move on if type is same else add to diff.modified with or without parent depending on appendParent

    // if complex do BatchSchemaDiffer.withParent(complexField.topLevel).diff(leftComplex.getChildren(), right.getChildren())
    // and add all fields to diff.modified with parent if appendParent is true

    BatchSchemaDiff diff = new BatchSchemaDiff();

    Map<String,Field> leftFieldMap = new LinkedHashMap<>();
    Map<String,Field> rightFieldMap = new LinkedHashMap<>();

    for (Field field : left) {
      leftFieldMap.put(field.getName().toLowerCase(), field);
    }

    for (Field field : right) {
      rightFieldMap.put(field.getName().toLowerCase(), field);
    }

    List<Field> deletedFields = Maps.difference(leftFieldMap, rightFieldMap).entriesOnlyOnLeft().values().stream().collect(Collectors.toList());
    deletedFields = deletedFields.stream().flatMap(x -> {
      if(parent != null) {
        return appendParent(parent, x).stream();
      }
      if(x.getChildren().size() > 0) {
        return appendParents(x, x.getChildren()).stream();
      }
      return Arrays.asList(x).stream();
    }).collect(Collectors.toList());

    diff.droppedField(deletedFields);

    List<Field> addedFields = Maps.difference(leftFieldMap, rightFieldMap).entriesOnlyOnRight().values().stream().collect(Collectors.toList());

    addedFields = addedFields.stream().flatMap(x -> {
      if(parent != null) {
        return appendParent(parent, x).stream();
      }
      if(x.getChildren().size() > 0) {
        return appendParents(x, x.getChildren()).stream();
      }
      return Arrays.asList(x).stream();
    }).collect(Collectors.toList());

    diff.addedField(addedFields);

    //any 2 fields with the same name are common fields. Type may have
    //changed.
    rightFieldMap.keySet().retainAll(leftFieldMap.keySet());
    List<Field> commonFields = rightFieldMap.values().stream().collect(Collectors.toList());

    for(Field f : commonFields) {
      if(f.getType().isComplex() ^ leftFieldMap.get(f.getName().toLowerCase()).getType().isComplex()) {
        throw UserException.invalidMetadataError().
          message(String.format("Field %s and %s should either both be simple or both be complex", leftFieldMap.get(f.getName().toLowerCase()), f.getType()))
          .buildSilently();
      }

      boolean isSimple = !f.getType().isComplex();
      //Simple field with types not same.
      if(isSimple) {
        if(!leftFieldMap.get(f.getName().toLowerCase()).getType().equals(f.getType())) {
          diff.modifiedField(appendParent(parent, f));
        }
      } else {
        //Is a complex field
        BatchSchemaDiff batchSchemaDiff = BatchSchemaDiffer.withParent(f).diff(leftFieldMap.get(f.getName().toLowerCase()).getChildren(), f.getChildren());
        diff.addedField(batchSchemaDiff.getAddedFields().stream().flatMap(x -> {
          if(parent != null) {
            return appendParent(parent, x).stream();
          }
          return Arrays.asList(x).stream();
        }).collect(Collectors.toList()));

        diff.droppedField(batchSchemaDiff.getDroppedFields().stream().flatMap(x -> {
          if(parent != null) {
            return appendParent(parent, x).stream();
          }
          return Arrays.asList(x).stream();
        }).collect(Collectors.toList()));

        diff.modifiedField(batchSchemaDiff.getModifiedFields().stream().flatMap(x -> {
          if(parent != null) {
            return appendParent(parent, x).stream();
          }
          return Arrays.asList(x).stream();
        }).collect(Collectors.toList()));
      }
    }
    return diff;
  }

  private List<Field> appendParent(Field parent, Field child) {
    if(!child.getType().isComplex()) {
      if(parent == null) {
        //return the child if there is not parent
        return ImmutableList.of(child);
      }
      return ImmutableList.of(new Field(parent.getName(), parent.getFieldType(), ImmutableList.of(child)));
    } else {
      return child.getChildren().stream().map(x -> {
        return new Field(parent.getName(), parent.getFieldType(), appendParent(child, x));
      }).collect(Collectors.toList());
    }
  }

  private List<Field> appendParents(Field parent, List<Field> child) {
    List<Field> children = new ArrayList();
    for(Field ele : child) {
      children.addAll(appendParent(parent, ele));
    }
    return children;
  }
}
