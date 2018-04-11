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
package com.dremio.datastore;

import java.util.Objects;

/**
 * StoreBuilder configuration.
 */
public final class StoreBuilderConfig {
  private String keySerializerClassName;
  private String valueSerializerClassName;
  private String documentConverterClassName;
  private String versionExtractorClassName;
  private String name;

  public StoreBuilderConfig() {
  }

  public String getKeySerializerClassName() {
    return keySerializerClassName;
  }

  public void setKeySerializerClassName(String keySerializerClassName) {
    this.keySerializerClassName = keySerializerClassName;
  }

  public String getValueSerializerClassName() {
    return valueSerializerClassName;
  }

  public void setValueSerializerClassName(String valueSerializerClassName) {
    this.valueSerializerClassName = valueSerializerClassName;
  }

  public String getDocumentConverterClassName() {
    return documentConverterClassName;
  }

  public void setDocumentConverterClassName(String documentConverterClassName) {
    this.documentConverterClassName = documentConverterClassName;
  }

  public String getVersionExtractorClassName() {
    return versionExtractorClassName;
  }

  public void setVersionExtractorClassName(String versionExtractorClassName) {
    this.versionExtractorClassName = versionExtractorClassName;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return new StringBuilder()
      .append("name: " + name)
      .append(", key serializer:" + keySerializerClassName)
      .append(", value serializer: " + valueSerializerClassName)
      .append(", version extractor: " + versionExtractorClassName)
      .append(", document converter: " + documentConverterClassName).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, keySerializerClassName, valueSerializerClassName, versionExtractorClassName, documentConverterClassName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }
    final StoreBuilderConfig that = (StoreBuilderConfig) obj;
    return Objects.equals(this.keySerializerClassName, that.keySerializerClassName) &&
        Objects.equals(this.valueSerializerClassName, that.valueSerializerClassName) &&
        Objects.equals(this.versionExtractorClassName, that.versionExtractorClassName) &&
        Objects.equals(this.documentConverterClassName, that.documentConverterClassName) &&
        Objects.equals(this.name, that.name);
  }
}
