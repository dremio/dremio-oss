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
package com.dremio.service.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * A type indicator similar to Calcite's convention concept that allows Dremio
 * to distinguish between types of Storage Plugins. Would be an ENUM if all
 * plugins were in the same place. However, we want this interface to be
 * extensible.
 */
public final class StoragePluginType {

  private final String name;
  private final Class<?> rulesFactoryClass;

  @JsonCreator
  public StoragePluginType(
      @JsonProperty("name") String name,
      @JsonProperty("rulesFactoryClass") Class<?> rulesFactory
      ) {
    this.name = Preconditions.checkNotNull(name);
    this.rulesFactoryClass = rulesFactory;
  }

  @Override
  @JsonProperty("name")
  public String toString() {
    return name;
  }

  /**
   * Defines a rules factory class that exposes rules that should be created
   * once for the given storage plugin type. Allows rules sharing across
   * multiple instance of the same type. Done through a class interface to avoid
   * references from Rules back to sources.
   *
   * Note that this should return a class of type StoragePluginTypeRulesFactory.
   * However, given the nature of our current modules, we can't add that to the
   * interface here.
   *
   * @return A rules factory class that extends StoragePluginTypeRulesFactory.
   */
  public Class<?> getRulesFactoryClass(){
    return rulesFactoryClass;
  }

  public String generateRuleName(String description){
    return description + ":" + name;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof StoragePluginType)) {
      return false;
    }
    StoragePluginType castOther = (StoragePluginType) other;
    return Objects.equal(name, castOther.name) && Objects.equal(rulesFactoryClass, castOther.rulesFactoryClass);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, rulesFactoryClass);
  }


}
