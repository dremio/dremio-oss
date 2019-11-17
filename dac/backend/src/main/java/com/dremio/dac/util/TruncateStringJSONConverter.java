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
package com.dremio.dac.util;


import com.fasterxml.jackson.databind.util.StdConverter;
import com.google.common.base.Strings;

/**
 * A JSON serialization converter that truncates strings
 */
public abstract class TruncateStringJSONConverter extends StdConverter<String, String> {
  private final int maxLength;

  public TruncateStringJSONConverter(int maxLength) {
    super();
    if (maxLength <= 0) {
      throw new IllegalArgumentException("maxLength must be greater then 0");
    }
    this.maxLength = maxLength;
  }

  @Override
  public String convert(String value) {
    if (Strings.isNullOrEmpty(value) || value.length() <= maxLength) {
      return value;
    }
    return String.format("%s...", value.substring(0, maxLength - 1));
  }
}
