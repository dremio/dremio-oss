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
package com.dremio.exec.store.iceberg;

import com.dremio.common.map.CaseInsensitiveImmutableBiMap;

/**
 * Middle-man between a field name and the field ID.
 */
public interface FieldIdBroker {
  int get(String fieldName);

  /**
   * Generates an unbounded sequence of field IDs, based on a perpetually increasing counter.
   */
  class UnboundedFieldIdBroker implements FieldIdBroker {
    private int id = 0;

    public int get(String fieldName) {
      int curid = id;
      id++;
      return curid;
    }
  }

  /**
   * Fetches the field ID corresponding to a full field name, based on the name to ID mapping fed to it during initialization.
   */
  class SeededFieldIdBroker implements FieldIdBroker {
    private final CaseInsensitiveImmutableBiMap<Integer> fieldIdMapping;

    public SeededFieldIdBroker(CaseInsensitiveImmutableBiMap<Integer> fieldIdMapping) {
      this.fieldIdMapping = fieldIdMapping;
    }

    public int get(String fieldName) {
      Integer fieldId = fieldIdMapping.get(fieldName);
      if (fieldId == null) {
        throw new IllegalStateException("Did not find a ID for field: " + fieldName);
      }
      return fieldId;
    }
  }
}
