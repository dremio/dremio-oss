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
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;

/**
 * POJO for storing batch schema differences.
 */
public class BatchSchemaDiff {

  private final List<Field> addedFields;
  private final List<Field> droppedFields;
  private final List<Field> modifiedFields;

  public BatchSchemaDiff() {
    addedFields = new ArrayList<>();
    droppedFields = new ArrayList<>();
    modifiedFields = new ArrayList<>();
  }

  public void addedField(List<Field> add) {
    addedFields.addAll(add);
  }

  public void droppedField(List<Field> drop) {
    droppedFields.addAll(drop);
  }

  public void modifiedField(List<Field> modified) {
    modifiedFields.addAll(modified);
  }

  public List<Field> getAddedFields() {
    return addedFields;
  }

  public List<Field> getDroppedFields() {
    return droppedFields;
  }

  public List<Field> getModifiedFields() {
    return modifiedFields;
  }
}
