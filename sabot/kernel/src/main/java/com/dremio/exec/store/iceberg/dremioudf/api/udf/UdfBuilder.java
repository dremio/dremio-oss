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

import com.dremio.exec.store.iceberg.dremioudf.api.catalog.UdfCatalog;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * A builder used to create or replace a SQL {@link Udf}.
 *
 * <p>Call {@link UdfCatalog#buildUdf(TableIdentifier)} to create a new builder.
 */
public interface UdfBuilder extends VersionBuilder<UdfBuilder> {

  /**
   * Add key/value properties to the UDF.
   *
   * @param properties key/value properties
   * @return this for method chaining
   */
  UdfBuilder withProperties(Map<String, String> properties);

  /**
   * Add a key/value property to the UDF.
   *
   * @param key a key
   * @param value a value
   * @return this for method chaining
   */
  UdfBuilder withProperty(String key, String value);

  /**
   * Sets a location for the UDF
   *
   * @param location the location to set for the UDF
   * @return this for method chaining
   */
  default UdfBuilder withLocation(String location) {
    throw new UnsupportedOperationException("Setting a UDF's location is not supported");
  }

  /**
   * Create the UDF.
   *
   * @return the UDF created
   */
  Udf create();

  /**
   * Replace the UDF.
   *
   * @return the {@link Udf} replaced
   */
  Udf replace();

  /**
   * Create or replace the UDF.
   *
   * @return the {@link Udf} created or replaced
   */
  Udf createOrReplace();
}
