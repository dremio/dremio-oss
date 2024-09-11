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
package com.dremio.sabot;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * This class represents the field to be passed to TypeDataGenerator so the generator knows the type
 * of arrow vector it has to create and fill with data, number of unique values in that data (to
 * control data skew) and how to sort the generated data.
 */
public class FieldInfo {
  private final Field field;

  private final int numOfUniqueValues;
  SortOrder sortOrder;

  public FieldInfo(String name, ArrowType arrowType, int numOfUniqueValues, SortOrder sortOrder) {
    this.field = new Field(name, new FieldType(true, arrowType, null), null);
    this.numOfUniqueValues = numOfUniqueValues;
    this.sortOrder = sortOrder;
  }

  public int getNumOfUniqueValues() {
    return numOfUniqueValues;
  }

  public Field getField() {
    return field;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }

  public enum SortOrder {
    ASCENDING,
    DESCENDING,
    RANDOM
  }
}
