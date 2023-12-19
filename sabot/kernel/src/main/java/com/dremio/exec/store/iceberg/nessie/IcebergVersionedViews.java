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
package com.dremio.exec.store.iceberg.nessie;

import java.util.List;
import java.util.Map;

import org.apache.iceberg.viewdepoc.View;
import org.apache.iceberg.viewdepoc.ViewDefinition;

import com.dremio.catalog.model.ResolvedVersionContext;

/**
 * Generic interface for operating on an view implementation.
 */
public interface IcebergVersionedViews {
  /**
   * Create a view without replacing any existing view.
   *
   * @param viewKey The list of view key elements.
   * @param viewDefinition SQL metadata of the view.
   * @param properties Version property genie-id of the operation, as well as table properties such
   *     as owner, table type, common view flag etc.
   * @param version The version to use.
   */
  void create(
      List<String> viewKey,
      ViewDefinition viewDefinition,
      Map<String, String> properties,
      ResolvedVersionContext version);

  /**
   * Replaces a view.
   *
   * @param viewKey The list of view key elements.
   * @param viewDefinition SQL metadata of the view
   * @param properties Version property genie-id of the operation, as well as table properties such
   *     as owner, table type, common view flag etc.
   * @param version The version to use.
   */
  void replace(
      List<String> viewKey,
      ViewDefinition viewDefinition,
      Map<String, String> properties,
      ResolvedVersionContext version);

  /**
   * Loads a view by name.
   *
   * @param viewKey The list of view key elements.
   * @param version The version to use.
   * @return All the metadata of the view
   */
  View load(List<String> viewKey, ResolvedVersionContext version);

  /**
   * Loads a view definition by name.
   *
   * @param viewKey The list of view key elements.
   * @param version The version to use.
   * @return SQL metadata of the view
   */
  ViewDefinition loadDefinition(List<String> viewKey, ResolvedVersionContext version);

  /**
   * Drops a view.
   *
   * @param viewKey The list of view key elements.
   * @param version The version to use.
   */
  void drop(List<String> viewKey, ResolvedVersionContext version);
}
