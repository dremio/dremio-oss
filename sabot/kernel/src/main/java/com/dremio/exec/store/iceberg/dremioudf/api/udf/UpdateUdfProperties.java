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

import java.util.Map;
import org.apache.iceberg.PendingUpdate;

/**
 * API for updating UDF properties.
 *
 * <p>Apply returns the updated UDF properties as a map for validation.
 *
 * <p>When committing, these changes will be applied to the current UDF metadata. Commit conflicts
 * will be resolved by applying the pending changes to the new UDF metadata.
 */
public interface UpdateUdfProperties extends PendingUpdate<Map<String, String>> {

  /**
   * Add a key/value property to the UDF.
   *
   * @param key a String key
   * @param value a String value
   * @return this for method chaining
   * @throws NullPointerException If either the key or value is null
   */
  UpdateUdfProperties set(String key, String value);

  /**
   * Remove the given property key from the UDF.
   *
   * @param key a String key
   * @return this for method chaining
   * @throws NullPointerException If the key is null
   */
  UpdateUdfProperties remove(String key);
}
