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
package com.dremio.exec.store.parquet;

import com.dremio.common.logical.FormatPluginConfig;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Objects;

@JsonTypeName("parquet")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ParquetFormatConfig implements FormatPluginConfig {

  public boolean autoCorrectCorruptDates = true;

  /** Extension of files written out with config as part of CTAS. */
  public String outputExtension = "parquet";

  @Override
  public int hashCode() {
    return Objects.hash(outputExtension, autoCorrectCorruptDates);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    final ParquetFormatConfig other = (ParquetFormatConfig) obj;

    return Objects.equals(outputExtension, other.outputExtension)
        && (autoCorrectCorruptDates == other.autoCorrectCorruptDates);
  }

  @Override
  public String toString() {
    return "ParquetFormatConfig [autoCorrectCorruptDates="
        + autoCorrectCorruptDates
        + ", outputExtension="
        + outputExtension
        + "]";
  }
}
