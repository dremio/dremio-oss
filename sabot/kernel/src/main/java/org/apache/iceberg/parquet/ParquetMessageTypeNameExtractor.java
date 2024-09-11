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

import java.util.Set;
import org.apache.parquet.schema.MessageType;

/** This class extract all parquet filed names, including complex types. */
public class ParquetMessageTypeNameExtractor extends MessageTypeToType {
  public ParquetMessageTypeNameExtractor() {
    super(
        (name) -> {
          return 0;
        });
  }

  public static Set<String> getFieldNames(MessageType fileSchema) {
    ParquetMessageTypeNameExtractor extractor = new ParquetMessageTypeNameExtractor();
    return extractor.getNames(fileSchema);
  }

  private Set<String> getNames(MessageType fileSchema) {
    ParquetTypeVisitor.visit(fileSchema, this);
    return getAliases().keySet();
  }
}
