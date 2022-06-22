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

package com.dremio.service.flight.utils;

/**
 * Auxilary class to hold Flight SQL TypeInfo data.
 */
public class TypeInfo {
  private final String typeName;
  private final Integer dataType;
  private final Integer columnSize;
  private final String literalPrefix;
  private final String literalSuffix;
  private final String createParams;
  private final Integer nullable;
  private final Integer caseSensitive;
  private final Integer searchable;
  private final Integer unsignedAttribute;
  private final Integer fixedPrecScale;
  private final Integer autoUniqueValue;
  private final String localTypeName;
  private final Integer minimumScale;
  private final Integer maximumScale;
  private final Integer sqlDataType;
  private final Integer sqlDatetimeSub;
  private final Integer numPrecRadix;

  public TypeInfo(String typeName, Integer dataType, Integer columnSize, String literalPrefix, String literalSuffix,
                  String createParams, Integer nullable, Integer caseSensitive, Integer searchable,
                  Integer unsignedAttribute, Integer fixedPrecScale, Integer autoUniqueValue, String localTypeName,
                  Integer minimumScale, Integer maximumScale, Integer sqlDataType, Integer sqlDatetimeSub,
                  Integer numPrecRadix) {
    this.typeName = typeName;
    this.dataType = dataType;
    this.columnSize = columnSize;
    this.literalPrefix = literalPrefix;
    this.literalSuffix = literalSuffix;
    this.createParams = createParams;
    this.nullable = nullable;
    this.caseSensitive = caseSensitive;
    this.searchable = searchable;
    this.unsignedAttribute = unsignedAttribute;
    this.fixedPrecScale = fixedPrecScale;
    this.autoUniqueValue = autoUniqueValue;
    this.localTypeName = localTypeName;
    this.minimumScale = minimumScale;
    this.maximumScale = maximumScale;
    this.sqlDataType = sqlDataType;
    this.sqlDatetimeSub = sqlDatetimeSub;
    this.numPrecRadix = numPrecRadix;
  }

  public String getTypeName() {
    return typeName;
  }

  public Integer getDataType() {
    return dataType;
  }

  public Integer getColumnSize() {
    return columnSize;
  }

  public String getLiteralPrefix() {
    return literalPrefix;
  }

  public String getLiteralSuffix() {
    return literalSuffix;
  }

  public String getCreateParams() {
    return createParams;
  }

  public Integer getNullable() {
    return nullable;
  }

  public Integer getCaseSensitive() {
    return caseSensitive;
  }

  public Integer getSearchable() {
    return searchable;
  }

  public Integer getUnsignedAttribute() {
    return unsignedAttribute;
  }

  public Integer getFixedPrecScale() {
    return fixedPrecScale;
  }

  public Integer getAutoUniqueValue() {
    return autoUniqueValue;
  }

  public String getLocalTypeName() {
    return localTypeName;
  }

  public Integer getMinimumScale() {
    return minimumScale;
  }

  public Integer getMaximumScale() {
    return maximumScale;
  }

  public Integer getSqlDataType() {
    return sqlDataType;
  }

  public Integer getSqlDatetimeSub() {
    return sqlDatetimeSub;
  }

  public Integer getNumPrecRadix() {
    return numPrecRadix;
  }
}
