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

import java.util.Map;

import org.apache.iceberg.DremioIndexByName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import com.dremio.common.map.CaseInsensitiveMap;

/**
 * Class contains miscellaneous utility functions for Iceberg table operations
 */
public class IcebergUtils {

  /**
   *
   * @param schema iceberg schema
   * @return column name to integer ID mapping
   */
  public static Map<String, Integer> getIcebergColumnNameToIDMap(Schema schema) {
    Map<String, Integer> schemaNameIDMap = TypeUtil.visit(Types.StructType.of(schema.columns()), new DremioIndexByName());
    return CaseInsensitiveMap.newImmutableMap(schemaNameIDMap);
  }
}
