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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;

/**
 * Provides an interface to create/update/drop Iceberg Views The implementation of this interface
 * will handle any differences between versions of Iceberg views
 */
public interface IcebergViewOperations {

  /**
   * @param viewKey Key of the view being created represented as a list of path components
   * @param viewSql SQL string definition of the view
   * @param viewSchema {@link Schema} of the view in IcebergView format
   * @param schemaPath The relative path of the view definition. This will be used to resolve the
   *     tables and views specified in the view SQL
   * @param resolvedVersionContext - This is the branch (with resolved commit hash {@link
   *     ResolvedVersionContext} ) where the view will be created
   */
  void create(
      List<String> viewKey,
      String viewSql,
      Schema viewSchema,
      List<String> schemaPath,
      ResolvedVersionContext resolvedVersionContext);

  /**
   * Updates an existing view with a new definition or properties
   *
   * @param viewKey Key of the view being created represented as a list of path components
   * @param viewSql** SQL string definition of the view
   * @param viewSchema** {@link Schema} of the view in IcebergView format
   * @param schemaPath** The relative path of the view definition. This will be used to resolve the
   *     tables and views specified in the view SQL
   * @param viewProperties** Properties of the view being updated
   * @param resolvedVersion !!Note!! : The parameters marked with '**' are the ones that can be
   *     updated. Whatever is passed in, will be replaced in the existing view. If the intent is to
   *     modify only one of them, then the other attributes should be the same as the value in the
   *     original view definition.
   */
  void update(
      List<String> viewKey,
      String viewSql,
      Schema viewSchema,
      List<String> schemaPath,
      Map<String, String> viewProperties,
      ResolvedVersionContext resolvedVersion);

  /**
   * Drops a view.
   *
   * @param viewKey Key of the view being created represented as a list of path components
   * @param version The version to use.
   */
  void drop(List<String> viewKey, ResolvedVersionContext version);

  IcebergViewMetadata refreshFromMetadataLocation(String metadataLocation);

  IcebergViewMetadata refresh(List<String> viewKey, ResolvedVersionContext version);
}
