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
package com.dremio.exec.catalog;

import org.apache.calcite.schema.TranslatableTable;

import com.dremio.service.namespace.NamespaceKey;

/**
 * Dremio translatable table.
 * <p>
 * Extension to {@link TranslatableTable} that provides canonical path of the table.
 */
public interface DremioTranslatableTable extends TranslatableTable {

  /**
   * Canonical path of the table. Note that this may be different from what was requested (both in
   * casing and components) depending on the behavior of the underlying source.
   *
   * @return canonical path
   */
  NamespaceKey getPath();

}
