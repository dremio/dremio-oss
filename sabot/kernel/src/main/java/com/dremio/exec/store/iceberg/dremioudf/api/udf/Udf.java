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
package com.dremio.exec.store.iceberg.dremioudf.api.udf;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.UpdateLocation;

/** Interface for udf definition. */
public interface Udf {

  String name();

  /**
   * Get the current version for this UDF, or null if there are no versions.
   *
   * @return the current UDF version.
   */
  UdfVersion currentVersion();

  /**
   * Get the versions of this UDF.
   *
   * @return an Iterable of versions of this UDF.
   */
  Iterable<UdfVersion> versions();

  /**
   * Get a version in this UDF by ID.
   *
   * @param versionId version ID
   * @return a version, or null if the ID cannot be found
   */
  UdfVersion version(String versionId);

  /**
   * Get the versions of this UDF signatures.
   *
   * @return an Iterable of {@Link Signature}.
   */
  Iterable<UdfSignature> signatures();

  /**
   * Get a version of signature in this UDF by ID.
   *
   * @param signatureId signature ID
   * @return a signature, or null if the ID cannot be found
   */
  UdfSignature signature(String signatureId);

  /**
   * Get the version history of this table.
   *
   * @return a list of {@link UdfHistoryEntry}
   */
  List<UdfHistoryEntry> history();

  /**
   * Return a map of string properties for this UDF.
   *
   * @return this UDF's properties map
   */
  Map<String, String> properties();

  /**
   * Return the UDF's base location.
   *
   * @return this UDF's location
   */
  default String location() {
    throw new UnsupportedOperationException("Retrieving a UDF's location is not supported");
  }

  /**
   * Create a new {@link UpdateUdfProperties} to update UDF properties.
   *
   * @return a new {@link UpdateUdfProperties}
   */
  UpdateUdfProperties updateProperties();

  /**
   * Create a new {@link ReplaceUdfVersion} to replace the UDF's current version.
   *
   * @return a new {@link ReplaceUdfVersion}
   */
  default ReplaceUdfVersion replaceVersion() {
    throw new UnsupportedOperationException("Replacing a UDF's version is not supported");
  }

  /**
   * Create a new {@link UpdateLocation} to set the UDF's location.
   *
   * @return a new {@link UpdateLocation}
   */
  default UpdateLocation updateLocation() {
    throw new UnsupportedOperationException("Updating a UDF's location is not supported");
  }

  /**
   * Returns the UDF's UUID
   *
   * @return the UDF's UUID
   */
  default UUID uuid() {
    throw new UnsupportedOperationException("Retrieving a UDF's uuid is not supported");
  }

  /**
   * Returns the UDF representation for the given SQL dialect
   *
   * @return the UDF representation for the given SQL dialect, or null if no representation could be
   *     resolved
   */
  default SQLUdfRepresentation sqlFor(String dialect) {
    throw new UnsupportedOperationException(
        "Resolving a sql with a given dialect is not supported");
  }
}
