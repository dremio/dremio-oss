/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.physical.visitor;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.parquet.Preconditions;

import com.dremio.exec.catalog.StoragePluginId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata about Dictionary encoded field.
 */
public class GlobalDictionaryFieldInfo {
  private final long dictionaryVersion;
  private final StoragePluginId storagePluginId;
  private final ArrowType arrowType;
  private final String dictionaryPath; // dictionary path based on field name from scan and dictionary version, remains constant throughout.
  private final String fieldName; // this may be changed during planning (ex, Project may change column a to a0)
  private final RelDataTypeField relDataTypeField; // not serialized

  @JsonCreator
  public GlobalDictionaryFieldInfo(
    @JsonProperty("dictionaryVersion") long dictionaryVersion,
    @JsonProperty("fieldName") String fieldName,
    @JsonProperty("storagePluginId") StoragePluginId storagePluginId,
    @JsonProperty("arrowType") ArrowType arrowType,
    @JsonProperty("dictionaryPath") String dictionaryPath) {
    this(dictionaryVersion, fieldName, storagePluginId, arrowType, dictionaryPath, null);
  }

  public GlobalDictionaryFieldInfo(
    long dictionaryVersion,
    String fieldName,
    StoragePluginId storagePluginId,
    ArrowType arrowType,
    String dictionaryPath,
    RelDataTypeField relDataTypeField) {
    Preconditions.checkArgument(dictionaryVersion >= 0, "dictionaryVersion must be >= 0");
    this.dictionaryVersion = dictionaryVersion;
    this.storagePluginId = storagePluginId;
    this.arrowType = arrowType;
    this.dictionaryPath = dictionaryPath;
    this.fieldName = fieldName;
    this.relDataTypeField = relDataTypeField;
  }

  public GlobalDictionaryFieldInfo withName(String fieldName) {
    return new GlobalDictionaryFieldInfo(dictionaryVersion, fieldName, storagePluginId, arrowType, dictionaryPath, relDataTypeField);
  }

  public long getDictionaryVersion() {
    return dictionaryVersion;
  }

  public StoragePluginId getStoragePluginId() {
    return storagePluginId;
  }

  public ArrowType getArrowType() {
    return arrowType;
  }

  public String getDictionaryPath() {
    return dictionaryPath;
  }

  public String getFieldName() {
    return fieldName;
  }

  @JsonIgnore
  public RelDataTypeField getRelDataTypeField() {
    return relDataTypeField;
  }
}
