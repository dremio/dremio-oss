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
import org.apache.iceberg.catalog.Namespace;

/**
 * A version of the UDF at a point in time.
 *
 * <p>A version consists of a UDF metadata file.
 *
 * <p>Versions are created by UDF operations, like Create and Replace.
 */
public interface UdfVersion {

  /** Return this version's id */
  String versionId();

  /** Return this version's signature id. Version id is UUID. */
  String signatureId();

  /**
   * Return this version's timestamp.
   *
   * <p>This timestamp is the same as those produced by {@link System#currentTimeMillis()}.
   *
   * @return a long timestamp in milliseconds
   */
  long timestampMillis();

  /**
   * Return the version summary
   *
   * @return a version summary
   */
  Map<String, String> summary();

  /**
   * Return the list of other UDF representations.
   *
   * <p>May contain SQL UDF representations for other dialects.
   *
   * @return the list of UDF representations
   */
  List<UdfRepresentation> representations();

  /** The default catalog when the UDF is created. */
  default String defaultCatalog() {
    return null;
  }

  /** The default namespace to use when the SQL does not contain a namespace. */
  Namespace defaultNamespace();
}
