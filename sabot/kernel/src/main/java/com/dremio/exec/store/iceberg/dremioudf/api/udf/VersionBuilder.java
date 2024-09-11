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

import org.apache.iceberg.catalog.Namespace;

public interface VersionBuilder<T> {

  /**
   * Set the UDF signature.
   *
   * @param signature The signature of the UDF for this version
   * @return this for method chaining
   */
  T withSignature(UdfSignature signature);

  /**
   * Add a UDF representation for the given dialect and the SQL body to the UDF.
   *
   * @param dialect The dialect of the UDF representation
   * @param body The SQL body of the UDF representation
   * @param comment Comment for the representation
   * @return this for method chaining
   */
  T withBody(String dialect, String body, String comment);

  /**
   * Set the default catalog to use for the UDF.
   *
   * @param catalog The default catalog to use when the SQL does not contain a catalog
   * @return this for method chaining
   */
  T withDefaultCatalog(String catalog);

  /**
   * Set the default namespace to use for the UDF.
   *
   * @param namespace The default namespace to use when the SQL does not contain a namespace
   * @return this for method chaining
   */
  T withDefaultNamespace(Namespace namespace);
}
