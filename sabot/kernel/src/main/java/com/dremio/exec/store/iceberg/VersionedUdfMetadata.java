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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public interface VersionedUdfMetadata {
  /**
   * Currently supported community Udf versions by Dremio. Add a new enum here for the next
   * supported Iceberg udf version
   */
  public static enum SupportedVersionedUdfSpecVersion {
    V0, // private implementation - only used by Dremio
    UNSUPPORTED;

    public static SupportedVersionedUdfSpecVersion of(int icebergFormatVersion) {
      switch (icebergFormatVersion) {
        case 0:
          return V0;
        default:
          return UNSUPPORTED;
      }
    }
  };

  /**
   * This is the set of dialects that Dremio engine can read from Note that the order in which this
   * enum is defined is important. When iterating through the various Udfrepresentations in Iceberg,
   * Dremio engine will search for the first representation going by the order of this list.
   */
  public static enum SupportedUdfDialects {
    DREMIOSQL("DremioSQL");
    private String name;

    SupportedUdfDialects(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * UUID of the UDF
   *
   * @return
   */
  String getUniqueId();

  /**
   * SQL dialect of the UDF
   *
   * @return
   */
  String getDialect();

  /**
   * Function body definition
   *
   * @return
   */
  String getBody();

  /**
   * @return Type
   */
  Type getReturnType();

  List<Types.NestedField> getParameters();

  SupportedVersionedUdfSpecVersion getFormatVersion();

  String getComment();

  List<String> getSchemaPath();

  /**
   * Warehouse location for the UDF metadata. This is the root location where the metadata json file
   * for the UDF would exist
   *
   * @return
   */
  String getLocation();

  /**
   * Metadata Json location for the UDF definition
   *
   * @return
   */
  String getMetadataLocation();

  /**
   * Map of properties associated with the udf
   *
   * @return
   */
  Map<String, String> getProperties();

  /**
   * Create timestamp for the UDF
   *
   * @return
   */
  long getCreatedAt();

  /**
   * Last modified timestamp when the UDF was altered
   *
   * @return
   */
  long getLastModifiedAt();

  /**
   * Json representation of the UDF metadata
   *
   * @return
   */
  String toJson();

  /**
   * Creates the udf metadata from the stored json definition.
   *
   * @param json
   * @return
   */
  VersionedUdfMetadata fromJson(String metadataLocation, String json);
}
