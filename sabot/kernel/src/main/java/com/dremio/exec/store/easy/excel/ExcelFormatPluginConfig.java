/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.easy.excel;

import com.dremio.common.logical.FormatPluginConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

@JsonTypeName("excel") @JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ExcelFormatPluginConfig implements FormatPluginConfig {

  private static final List<String> EXTENSIONS =
      ImmutableList.of(
          "xls", // Excel 97 - 2007 format.
          "xlsx" // Excel 2007 onwards format.
      );

  /**
   * These need to be public in order for the SELECT WITH OPTIONS to work.
   */
  public String sheet;
  public boolean extractHeader;
  public boolean hasMergedCells;
  public boolean xls; /** true if we are reading .xls, false if it's .xlsx */

  @JsonIgnore
  public List<String> getExtensions() {
    return EXTENSIONS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExcelFormatPluginConfig that = (ExcelFormatPluginConfig) o;

    return Objects.equal(sheet, that.sheet) &&
        extractHeader == that.extractHeader &&
        hasMergedCells == that.hasMergedCells &&
        xls == that.xls;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sheet, extractHeader, hasMergedCells, xls);
  }
}