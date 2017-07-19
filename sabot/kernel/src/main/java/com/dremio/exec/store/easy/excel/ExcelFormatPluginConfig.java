/*
 * Copyright 2016 Dremio Corporation
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