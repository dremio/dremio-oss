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

package com.dremio.exec.store.parquet;

import java.util.Map;
import java.util.Optional;

import com.dremio.common.expression.CompleteType;

/**
 * In case there are external schema definitions associated with Parquet, they should be supplied via this class's impl.
 */
public interface ManagedSchema {

  /**
   * Returns the field and type definition for a given field.
   * @param fieldName
   * @return
   */
  Optional<ManagedSchemaField> getField(String fieldName);

  /**
   * Returns all the fields in the schema
   * @return
   */
  Map<String, ManagedSchemaField> getAllFields();

  /**
   * Returns true if field exists, is of char types and has a fixed length
   * @param fieldName
   * @return
   */
  default boolean isNotValidFixedLenTextField(String fieldName) {
    final Optional<ManagedSchemaField> field = getField(fieldName);
    if (!field.isPresent()) {
      return true;
    }

    if (!field.get().isTextField() ||
      field.get().getLength() <= 0 || field.get().getLength() >= CompleteType.DEFAULT_VARCHAR_PRECISION) {
      return true;
    }

    return false;
  }
}
