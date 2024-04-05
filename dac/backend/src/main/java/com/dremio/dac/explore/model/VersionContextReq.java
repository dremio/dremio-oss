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
package com.dremio.dac.explore.model;

import com.dremio.dac.service.errors.ClientErrorException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import java.util.Locale;
import javax.annotation.Nullable;

/** Extracts the "references" key from the request payload */
public class VersionContextReq {

  public enum VersionContextType {
    BRANCH,
    TAG,
    COMMIT,
  }

  private final VersionContextType type;
  private final String value;

  @JsonCreator
  public VersionContextReq(
      @JsonProperty("type") VersionContextType type, @JsonProperty("value") String value) {
    this.type = type;
    this.value = value;
  }

  /**
   * Tries to create a VersionContextReq from input strings. Case-insensitive enum conversion.
   * Returns null on most failures.
   */
  public static @Nullable VersionContextReq tryParse(String type, String value) {
    if (Strings.isNullOrEmpty(type) && Strings.isNullOrEmpty(value)) {
      return null;
    } else if (Strings.isNullOrEmpty(type) && !Strings.isNullOrEmpty(value)) {
      throw new ClientErrorException("Version type was null while value was specified");
    } else if (!Strings.isNullOrEmpty(type) && Strings.isNullOrEmpty(value)) {
      throw new ClientErrorException("Version value was null while type was specified");
    }
    return new VersionContextReq(VersionContextType.valueOf(type.toUpperCase(Locale.ROOT)), value);
  }

  public VersionContextType getType() {
    return type;
  }

  public String getValue() {
    return value;
  }
}
