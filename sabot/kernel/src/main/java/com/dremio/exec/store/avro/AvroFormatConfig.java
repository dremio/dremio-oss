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
package com.dremio.exec.store.avro;

import com.dremio.common.logical.FormatPluginConfig;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Format plugin config for Avro data files.
 */
@JsonTypeName("avro")
public class AvroFormatConfig implements FormatPluginConfig {

  @Override
  public int hashCode() {
    // Pick a random hash code so that the format config is always the same.
    return 101;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof AvroFormatConfig;
  }
}
