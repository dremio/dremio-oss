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
package com.dremio.exec.store;

import static com.dremio.common.perf.Timer.time;

import java.util.List;

import org.apache.calcite.jdbc.ImplicitRootSchema;
import org.apache.calcite.schema.SchemaPlus;

import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.exec.proto.UserBitShared.PlanPhaseProfile;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig.SchemaInfoProvider;
import com.dremio.exec.store.ischema.InfoSchemaStoragePlugin;
import com.dremio.exec.store.sys.SystemTablePlugin;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.collect.Lists;

/**
 * Class which creates new schema trees.
 */
public class SchemaTreeProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaTreeProvider.class);

  private final SabotContext dContext;
  private final MetadataStatsCollector metadataStatsCollector;

  public SchemaTreeProvider(final SabotContext dContext) {
    this.dContext = dContext;
    this.metadataStatsCollector = new MetadataStatsCollector();
  }

  public MetadataStatsCollector getMetadataStatsCollector() {
    return metadataStatsCollector;
  }

  /**
   * Return root schema with schema owner as the given user.
   *
   * @param userName Name of the user who is accessing the storage sources.
   * @param provider {@link SchemaInfoProvider} instance
   * @return Root of the schema tree.
   */
  public SchemaPlus getRootSchema(final String userName, final SchemaInfoProvider provider) {

    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(userName).setProvider(provider).build();
    return getRootSchema(schemaConfig);
  }

  /**
   * Create and return a SchemaTree with given <i>schemaConfig</i>.
   * @param schemaConfig
   * @return
   */
  public SchemaPlus getRootSchema(SchemaConfig schemaConfig) {
    try (TimedBlock b = time("Create schema tree")) {
      final NamespaceService ns = dContext.getNamespaceService(schemaConfig.getUserName());
      final SchemaPlus rootSchemaPlus = new ImplicitRootSchema(ns, dContext, schemaConfig, metadataStatsCollector).plus();
      ((InfoSchemaStoragePlugin)dContext.getStorage().getPlugin(StoragePluginRegistry.INFORMATION_SCHEMA_PLUGIN)).registerSchemas(schemaConfig, rootSchemaPlus);
      ((SystemTablePlugin)dContext.getStorage().getPlugin(StoragePluginRegistry.SYS_PLUGIN)).registerSchemas(schemaConfig, rootSchemaPlus);

      if (schemaConfig.exposeSubSchemasAsTopLevelSchemas()) {
        final RootSchema rootSchema = rootSchemaPlus.unwrap(RootSchema.class);
        rootSchema.exposeSubSchemasAsTopLevelSchemas(rootSchemaPlus);
      }

      return rootSchemaPlus;
    } catch(Exception e) {
      // We can't proceed further without a schema, throw a runtime exception.
      final String errMsg = String.format("Failed to create schema tree: %s", e.getMessage());
      logger.error(errMsg, e);
      throw new RuntimeException(errMsg, e);
    }
  }

  public enum SchemaType {
    SOURCE, SPACE, HOME
  }

  public static class MetadataStatsCollector {
    private final List<PlanPhaseProfile> planPhaseProfiles = Lists.newArrayList();

    void addDatasetStat(String datasetPath, String type, long millisTaken) {
      planPhaseProfiles.add(PlanPhaseProfile.newBuilder()
        .setPhaseName(String.format("%s: %s", datasetPath, type))
        .setDurationMillis(millisTaken)
        .build());
    }

    public List<PlanPhaseProfile> getPlanPhaseProfiles() {
      return planPhaseProfiles;
    }
  }
}
