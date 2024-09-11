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
package com.dremio.exec.ops;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.service.namespace.NamespaceKey;
import java.util.Collection;
import java.util.Map;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;

/**
 * Catalog interface that planner uses for SQL to {@link org.apache.calcite.rel.RelNode} validation,
 * conversion and remaining logical planning phases.
 *
 * <p>This is the high level separation of concerns:
 *
 * <p>Planner Catalog - Creates {@link DremioCatalogReader} - Returns validated row types for views
 * by expanding them - Ensures fine grain access control is applied on converted view types -
 * Implements the resolve pattern to contextualize for query, sub-query and UDFs. Uses Calcite APIs.
 * - Handles cross source validation. See {@link
 * com.dremio.exec.catalog.CatalogOptions#DISABLE_CROSS_SOURCE_SELECT}
 *
 * <p>{@link Catalog} - Primarily a DAO for table, view, sources and function metadata. - Validates
 * RBAC on single objects. - Implements the resolve pattern to contextualize by user, schema and
 * source versions. - Has {@link com.dremio.exec.catalog.CachingCatalog} but this should move to
 * PlannerCatalog
 */
public interface PlannerCatalog {

  /**
   * Retrieve a table's definition, first checking the default schema. There is significant
   * performance overhead if schema is a remote source and key is an absolute path under a different
   * source. If key refers to a versioned source, then the table version will come from either
   * {@link MetadataRequestOptions#getSourceVersionMapping()} or the source's default branch
   * reference if not set.
   *
   * <p>This API does not enforce SELECT permissions, does not expand views and does not apply fine
   * grain access control (FGAC). Use it to check for table existence or table metadata but don't
   * try to validate and convert it
   *
   * @param key
   * @return DremioTable if found, otherwise null if table does not exist.
   */
  DremioTable getTableWithSchema(NamespaceKey key);

  /**
   * Similar to {@link #getTableWithSchema(NamespaceKey)} except the default schema is not checked
   * and thus the input key is an absolute path. This API is more performant than {@link
   * #getTableWithSchema(NamespaceKey)}
   *
   * @param key
   * @return DremioTable if found, otherwise null if table does not exist.
   */
  DremioTable getTableIgnoreSchema(NamespaceKey key);

  /**
   * Similar to {@link #getTableWithSchema(NamespaceKey)} except a version context is explicitly
   * specified.
   *
   * @param catalogEntityKey
   * @return DremioTable if found, otherwise null if table does not exist.
   */
  DremioTable getTableWithSchema(CatalogEntityKey catalogEntityKey);

  /**
   * Similar to {@link #getTableIgnoreSchema(NamespaceKey)} except a version context is explicitly
   * specified. This API is more performant.
   *
   * @param catalogEntityKey
   * @return DremioTable if found, otherwise null if table does not exist.
   */
  DremioTable getTableIgnoreSchema(CatalogEntityKey catalogEntityKey);

  /**
   * Retrieve's a table whose {@link org.apache.calcite.schema.Table#getRowType(RelDataTypeFactory)}
   * is guaranteed to match its converted type. This API can be expensive because it will expand
   * views but the converted rel node will be cached and re-used when the view table is later
   * converted.
   *
   * <p>This API will validate SELECT permission.
   *
   * <p>If FGAC is applied to a view, the returned DremioTable looks like: ViewTable <-
   * ConvertedViewTable <- FineGrainedAccessTable
   */
  DremioTable getValidatedTableWithSchema(NamespaceKey key);

  /**
   * Similar to {@link #getValidatedTableWithSchema(NamespaceKey)} except the version context is
   * specified.
   *
   * @param catalogEntityKey
   * @return DremioTable if found, otherwise null if table does not exist.
   */
  DremioTable getValidatedTableWithSchema(CatalogEntityKey catalogEntityKey);

  /**
   * Similar to {@link #getValidatedTableWithSchema(NamespaceKey)} except:
   *
   * <p>Default schema is not checked Does NOT validate SELECT permissions
   */
  DremioTable getValidatedTableIgnoreSchema(NamespaceKey key);

  /**
   * Get a list of functions. Provided specifically for DremioCatalogReader.
   *
   * @param path
   * @param functionType
   * @return
   */
  Collection<Function> getFunctions(CatalogEntityKey path, SimpleCatalog.FunctionType functionType);

  /**
   * @return all tables that have been requested from this catalog.
   */
  Iterable<DremioTable> getAllRequestedTables();

  /**
   * Clears all caches associated to a particular dataset. If versionContext is provided, the
   * resolved version context with commit hash will also be cleared.
   *
   * @param dataset
   * @param versionContext
   */
  void clearDatasetCache(final NamespaceKey dataset, final TableVersionContext versionContext);

  /**
   * Clears the converted view cache which is needed when we re-plan a REFRESH REFLECTION job with
   * default raw reflections enabled. This only clears the converted view cache and not the metadata
   * catalog cache which can still be re-used.
   */
  void clearConvertedCache();

  /**
   * Validate if there is a violation of cross source selection. {@link
   * com.dremio.exec.catalog.CatalogOptions#DISABLE_CROSS_SOURCE_SELECT}
   */
  void validateSelection();

  /**
   * Determine whether the container at the given path exists. See {@link
   * com.dremio.exec.catalog.EntityExplorer#containerExists(CatalogEntityKey)}
   */
  boolean containerExists(CatalogEntityKey path);

  /**
   * Get the default schema for this catalog. Returns null if there is no default.
   *
   * @return The default schema path.
   */
  NamespaceKey getDefaultSchema();

  JavaTypeFactory getTypeFactory();

  /**
   * Return a new PlannerCatalog whose underlying metadata {@link Catalog} is contextualized to the
   * provided default schema.
   *
   * @param newDefaultSchema
   * @return A new schema with the same user but with the newly provided default schema.
   */
  PlannerCatalog resolvePlannerCatalog(NamespaceKey newDefaultSchema);

  /**
   * Return a new PlannerCatalog whose underlying metadata {@link Catalog} is contextualized to the
   * provided subject
   *
   * @param subject
   * @return
   */
  PlannerCatalog resolvePlannerCatalog(CatalogIdentity subject);

  /**
   * Return a new PlannerCatalog whose underlying metadata {@link Catalog} is contextualized to the
   * provided version context
   *
   * @param sourceVersionMapping
   * @return A new catalog contextualized to the provided version context.
   */
  PlannerCatalog resolvePlannerCatalog(Map<String, VersionContext> sourceVersionMapping);

  /**
   * Return a new PlannerCatalog whose underlying metadata {@link Catalog} has been transformed by
   * the provided transformer
   */
  PlannerCatalog resolvePlannerCatalog(
      final java.util.function.Function<Catalog, Catalog> catalogTransformer);

  Catalog getMetadataCatalog();

  /**
   * Disposing planner state is critical to reduce memory usage anytime the plan lives beyond the
   * lifetime of a single query. This occurs with both plan cache and materialization cache.
   */
  void dispose();
}
