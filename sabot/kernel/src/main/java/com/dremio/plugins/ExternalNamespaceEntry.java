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
package com.dremio.plugins;

import java.util.List;

import org.apache.arrow.util.Preconditions;

public final class ExternalNamespaceEntry {
  public enum Type {
    FOLDER,
    ICEBERG
  }

  private Type type;
  private List<String> nameElements;

  private ExternalNamespaceEntry(Type type, List<String> nameElements) {
    Preconditions.checkNotNull(nameElements);
    Preconditions.checkArgument(nameElements.size() >= 1);
    this.type = type;
    this.nameElements = nameElements;
  }

  public static ExternalNamespaceEntry of(String type, List<String> nameElements) {
    Preconditions.checkNotNull(type);
    return new ExternalNamespaceEntry(mapType(type), nameElements);
  }

  public Type getType() {
    return type;
  }

  public List<String> getNameElements() {
    return nameElements;
  }

  public List<String> getNamespace() {
    return nameElements.subList(0, nameElements.size() - 1);
  }

  public String getName() {
    return nameElements.get(nameElements.size() - 1);
  }

  private static Type mapType(String type) {
    switch(type) {
      case "UNKNOWN":
        return Type.FOLDER;
      case "ICEBERG_TABLE":
        return Type.ICEBERG;
      default:
        throw new IllegalStateException("Unexpected value: " + type);
    }
  }
}
