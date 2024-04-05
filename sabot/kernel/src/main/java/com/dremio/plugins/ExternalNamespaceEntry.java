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

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.projectnessie.model.Content;

public final class ExternalNamespaceEntry {
  public enum Type {
    UNKNOWN,
    FOLDER,
    ICEBERG_TABLE,
    ICEBERG_VIEW;

    public Content.Type toNessieContentType() {
      switch (this) {
        case ICEBERG_TABLE:
          return Content.Type.ICEBERG_TABLE;
        case ICEBERG_VIEW:
          return Content.Type.ICEBERG_VIEW;
        case FOLDER:
          return Content.Type.NAMESPACE;
        default:
          throw new IllegalArgumentException("toNessieContentType failed: " + this);
      }
    }

    public static Type fromNessieContentType(Content.Type nessieContentType) {
      Preconditions.checkNotNull(nessieContentType);
      if (Content.Type.ICEBERG_TABLE.equals(nessieContentType)) {
        return Type.ICEBERG_TABLE;
      }
      if (Content.Type.ICEBERG_VIEW.equals(nessieContentType)) {
        return Type.ICEBERG_VIEW;
      }
      if (Content.Type.NAMESPACE.equals(nessieContentType)) {
        return Type.FOLDER;
      }
      return Type.UNKNOWN;
    }
  }

  private final Type type;
  private final List<String> nameElements;
  private final @Nullable String id;
  private final @Nullable TableVersionContext tableVersionContext;
  private final @Nullable Optional<NessieContent> nessieContent;

  private ExternalNamespaceEntry(
      Type type,
      List<String> nameElements,
      @Nullable String id,
      @Nullable TableVersionContext tableVersionContext,
      @Nullable Optional<NessieContent> nessieContent) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(nameElements);
    Preconditions.checkArgument(nameElements.size() >= 1);

    this.type = type;
    this.nameElements = nameElements;
    this.id = id;
    this.tableVersionContext = tableVersionContext;
    this.nessieContent = nessieContent;
  }

  public static ExternalNamespaceEntry of(Type type, List<String> nameElements) {
    return of(type, nameElements, null, null, null);
  }

  @VisibleForTesting
  public static ExternalNamespaceEntry of(
      Type type,
      List<String> nameElements,
      @Nullable String id,
      @Nullable TableVersionContext tableVersionContext) {
    return of(type, nameElements, id, tableVersionContext, null);
  }

  public static ExternalNamespaceEntry of(
      Type type,
      List<String> nameElements,
      @Nullable String id,
      @Nullable TableVersionContext tableVersionContext,
      @Nullable Optional<NessieContent> nessieContent) {
    return new ExternalNamespaceEntry(type, nameElements, id, tableVersionContext, nessieContent);
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

  public @Nullable String getId() {
    return id;
  }

  public @Nullable TableVersionContext getTableVersionContext() {
    return tableVersionContext;
  }

  /**
   * returns null when no content was requested. returns Optional instance when content was
   * requested.
   */
  public @Nullable Optional<NessieContent> getNessieContent() {
    return nessieContent;
  }

  @Override
  public String toString() {
    return "ExternalNamespaceEntry{"
        + "type="
        + type
        + ", nameElements="
        + nameElements
        + ", id="
        + id
        + ", tableVersionContext="
        + tableVersionContext
        + ", nessieContent="
        + nessieContent
        + '}';
  }
}
