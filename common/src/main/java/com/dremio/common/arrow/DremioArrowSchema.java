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
package com.dremio.common.arrow;

import java.io.IOException;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.serde.BackwardCompatibleSchemaDe;
import com.google.common.base.Preconditions;

/**
 * To be used instead of Arrow Schema to deserialize JSON based Schema
 * as it can contain old data that current version of Arrow (as of Dremio 2.1.0)
 * does not support
 */
public final class DremioArrowSchema {

  public static final String DREMIO_ARROW_SCHEMA = "dremio.arrow.schema";
  public static final String DREMIO_ARROW_SCHEMA_2_1 = "dremio.arrow.schema.2.1";

  /**
   * To parse Arrow Schema parsed from JSON
   * compatible with Arrow used in pre Dremio 2.1.0
   * @param json
   * @return
   * @throws IOException
   */
  public static Schema fromJSON(String json) throws IOException {
    return BackwardCompatibleSchemaDe.fromJSON(json);
  }

  /**
   * To parse Arrow Schema from JSON based on property
   * existing in Parquet Footer Metadata
   * @param properties
   * @return
   * @throws IOException
   */
  public static Schema fromMetaData(Map<String, String> properties) throws IOException {
    Preconditions.checkNotNull(properties);
    String jsonArrowSchema = properties.get(DREMIO_ARROW_SCHEMA);
    String jsonArrowSchema2_1 = properties.get(DREMIO_ARROW_SCHEMA_2_1);

    // check in order
    // DREMIO_ARROW_SCHEMA - if found it is pre 2.1.0 generated file - use it
    // if DREMIO_ARROW_SCHEMA is not found
    // check DREMIO_ARROW_SCHEMA_2_1
    // if found - it is 2.1.0+ generated file - use it

    if (jsonArrowSchema != null) {
      return fromJSON(jsonArrowSchema);
    }
    if (jsonArrowSchema2_1 != null) {
      return fromJSON(jsonArrowSchema2_1);
    }
    return null;
  }

  /**
   *
   * @param properties
   * @return true if parquet file footer properties contain arrow schema
   */
  public static boolean isArrowSchemaPresent(Map<String, String> properties) {
    Preconditions.checkNotNull(properties);
    String jsonArrowSchema = properties.get(DREMIO_ARROW_SCHEMA);
    String jsonArrowSchema2_1 = properties.get(DREMIO_ARROW_SCHEMA_2_1);
    return jsonArrowSchema != null || jsonArrowSchema2_1 != null;
  }
}
