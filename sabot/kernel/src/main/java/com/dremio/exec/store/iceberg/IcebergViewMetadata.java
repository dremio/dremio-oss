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
import org.apache.iceberg.Schema;

/**
 * Interface to provide iceberg view metadata (Implementations based on the View Spec Version) This
 * interface is used by the DataplanePlugin caching and Catalog layers to retrieve Iceberg view
 * metadata.
 */
public interface IcebergViewMetadata {

  /**
   * Currently supported Iceberg versions by Dremio. (See org/apache/iceberg/view/ViewMetadata.java
   * for officially supported Iceberg versions) Add a new enum here for the next supported Iceberg
   * view version
   */
  public static enum SupportedIcebergViewSpecVersion {
    V0, // private implementation - only used by Dremio
    V1, // First version officially supported by Iceberg
    UNKNOWN
  };

  public static SupportedIcebergViewSpecVersion of(int icebergViewSpecVersion) {
    switch (icebergViewSpecVersion) {
      case 1:
        return SupportedIcebergViewSpecVersion.V1;
      default:
        return SupportedIcebergViewSpecVersion.UNKNOWN;
    }
  }

  /**
   * This is the set of dialects that Dremio engine can read from Note that the order in which this
   * enum is defined is important. When iterating through the various View representations in
   * Iceberg, Dremio engine will search for the first representation going by the order of this
   * list.
   */
  public static enum SupportedViewDialectsForRead {
    DREMIO("DREMIO"), // Soon to be deprecated (V0 views used this value)
    DREMIOSQL("DremioSQL"),
    SPARK("Spark");
    private String name;

    SupportedViewDialectsForRead(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * This is the list of dialects which Dremio engine can update When we update a view we allow any
   * view with a dialect in this set to be updated.
   */
  public static enum SupportedViewDialectsForWrite {
    DREMIO("DREMIO"), // Soon to be deprecated (V0 views used this value)
    DREMIOSQL("DremioSQL");
    private String name;

    SupportedViewDialectsForWrite(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Get the view schema
   *
   * @return Schema in Iceberg format
   */
  SupportedIcebergViewSpecVersion getFormatVersion();

  Schema getSchema();

  /**
   * Returns the sql for the view as a String
   *
   * @return
   */
  String getSql();

  /**
   * The schema path that is used to resolve the view. Eg If the schema Path was stored as
   * "foo.folder1", when a view "v1" is expanded, v1 will get qualified as "foo.folder1.v1"
   *
   * @return
   */
  List<String> getSchemaPath();

  /**
   * Warehouse location for the view's metadata
   *
   * @return
   */
  String getLocation();

  /**
   * Metadata Json location for the view's definition
   *
   * @return
   */
  String getMetadataLocation();

  /**
   * UUID of the view
   *
   * @return
   */
  String getUniqueId();

  /**
   * Map of properties associated with the view
   *
   * @return
   */
  Map<String, String> getProperties();

  /**
   * Create timestamp for the view
   *
   * @return
   */
  long getCreatedAt();

  /**
   * Last modified timestamp when the view was altered
   *
   * @return
   */
  long getLastModifiedAt();

  /**
   * SQL dialect of the view
   *
   * @return
   */
  String getDialect();

  /**
   * Json representation of the view metadata
   *
   * @return
   */
  String toJson();

  /**
   * Creates the view metadata in whichever view format version is stored in the json.
   *
   * @param json
   * @return
   */
  IcebergViewMetadata fromJson(String metadataLocation, String json);
}
