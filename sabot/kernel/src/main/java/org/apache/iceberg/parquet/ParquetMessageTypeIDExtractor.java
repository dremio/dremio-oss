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
package org.apache.iceberg.parquet;

import org.apache.parquet.schema.MessageType;

/** This class reuses Iceberg's MessageTypeToType. */
public class ParquetMessageTypeIDExtractor extends MessageTypeToType {
  public ParquetMessageTypeIDExtractor() {
    super(null);
  }

  /**
   * Calling hasIds on instance of this class returns false if parquet file schema has no IDs
   * defined for columns. When it returns true, method, getAliases of this instance returns the
   * column to ID mapping
   *
   * @param fileSchema parquet file schema
   * @return true if parquet file schema has IDs for column names
   */
  public boolean hasIds(MessageType fileSchema) {
    try {
      // Try to convert the type to Iceberg. If an ID assignment is needed, return false.
      ParquetTypeVisitor.visit(fileSchema, this);

      // no assignment was needed
      return true;

    } catch (Exception e) {
      // at least one field was missing an id.
      return false;
    }
  }
}
