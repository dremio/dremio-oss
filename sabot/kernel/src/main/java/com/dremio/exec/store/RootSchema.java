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

import static com.dremio.exec.store.StoragePluginRegistryImpl.isInternal;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaTreeProvider.SchemaType;
import com.dremio.exec.store.SchemaTreeProvider.MetadataStatsCollector;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class RootSchema extends AbstractSchema {

  private final NamespaceService ns;
  private final SchemaConfig schemaConfig;
  private final SabotContext sabotContext;
  private final Supplier<Map<String, SourceConfig>> sources;
  private final Supplier<Set<String>> spaces;
  private final Supplier<Set<String>> allHomeSpaces;
  private final Supplier<Boolean> userHomeExists;
  private final MetadataStatsCollector metadataStatsCollector;
  private final boolean isSystemUser;

  public RootSchema(final NamespaceService ns, final SabotContext sabotContext, final SchemaConfig schemaConfig,
                    final MetadataStatsCollector metadataStatsCollector) {
    super(Collections.<String>emptyList(), "");
    this.ns = ns;
    this.sabotContext = sabotContext;
    this.schemaConfig = schemaConfig;
    this.metadataStatsCollector = metadataStatsCollector;
    this.isSystemUser = SYSTEM_USERNAME.equals(schemaConfig.getUserName());
    this.sources = Suppliers.memoize(new Supplier<Map<String, SourceConfig>>() {
      @Override
      public Map<String, SourceConfig> get() {
        final ImmutableMap.Builder<String, SourceConfig> builder = ImmutableMap.builder();
        for(final SourceConfig sourceConfig : ns.getSources()) {
          final String sourceName = sourceConfig.getName();
          if (!isInternal(sourceName) ||
              "__home".equalsIgnoreCase(sourceName) ||
              "$scratch".equalsIgnoreCase(sourceName) ||
              isSystemUser ||
              schemaConfig.exposeInternalSources()) {
            builder.put(sourceName, sourceConfig);
          }
        }
        return builder.build();
      }
    });
    this.spaces = Suppliers.memoize(new Supplier<Set<String>>() {
      @Override
      public Set<String> get() {
        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for(final SpaceConfig spaceConfig : ns.getSpaces()) {
          builder.add(spaceConfig.getName());
        }
        return builder.build();
      }
    });
    this.allHomeSpaces = Suppliers.memoize(new Supplier<Set<String>>() {
      @Override
      public Set<String> get() {
        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for(final HomeConfig homeConfig : ns.getHomeSpaces()) {
          builder.add("@" + homeConfig.getOwner());
        }
        return builder.build();
      }
    });
    this.userHomeExists = Suppliers.memoize(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        final NamespaceKey homePath = new NamespaceKey("@" + schemaConfig.getUserName());
        return ns.exists(homePath, NameSpaceContainer.Type.HOME);
      }
    });
  }

  @Override
  public String getTypeName() {
    return null;
  }

  @Override
  public Schema getSubSchema(String name) {
    // This gets called for same schema name multiple times. cache the result.
    final SimpleSchema directRootLevelSchema = findRootLevelSubSchema(name);
    if (directRootLevelSchema != null) {
      return directRootLevelSchema;
    }

    // If the name refers to schema subschema promoted to root level (possible when exposing schemas to BI tools,
    // because BI tools can not work with more than one level of subschema
    final List<String> subSchemaPath = SubSchemaWrapper.toSubSchemaPath(name);
    if (subSchemaPath.size() <= 1) {
      return null; // still a top-level schema as the path components contains just one.
    }
    final SimpleSchema rootLevelSchema = findRootLevelSubSchema(subSchemaPath.get(0));

    Schema schema = rootLevelSchema;
    for(int i = 1; i < subSchemaPath.size(); i++) {
      if (schema == null) {
        return null;
      }
      schema = schema.getSubSchema(subSchemaPath.get(i));
    }
    return schema;
  }

  private SimpleSchema findRootLevelSubSchema(String name) {
    if (sources.get().containsKey(name)) {
      StoragePlugin<?> plugin = null;
      try{
        plugin = sabotContext.getStorage().getPlugin(name);
      }catch(ExecutionSetupException e){
        logger.info("Failure while attempting to retrieve plugin for name {}.", name, e);
      }
      SourceConfig config = sources.get().get(name);
      MetadataPolicy policy = config.getMetadataPolicy() != null ? config.getMetadataPolicy() : CatalogService.DEFAULT_METADATA_POLICY;
      SimpleSchema.MetadataParam metadataParam = new SimpleSchema.MetadataParam(policy, config.getLastRefreshDate());
      return newTopLevelSchema(name, SchemaType.SOURCE, metadataParam, plugin == null ? SchemaMutability.NONE : plugin.getMutability());
    }

    if (spaces.get().contains(name)) {
      return newTopLevelSchema(name, SchemaType.SPACE, null, SchemaMutability.USER_VIEW);
    }

    if (isSystemUser && allHomeSpaces.get().contains(name)) {
      return newTopLevelSchema(name, SchemaType.HOME, null, SchemaMutability.USER_VIEW);
    }

    if (userHomeExists.get()) {
      final String homeName = "@" + schemaConfig.getUserName();
      if (homeName.equals(name)) {
        return newTopLevelSchema(homeName, SchemaType.HOME, null, SchemaMutability.USER_VIEW);
      }
    }

    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    // Convert this into an iterator
    final ImmutableSet.Builder<String> setBuilder = ImmutableSet.<String>builder()
        .addAll(sources.get().keySet())
        .addAll(spaces.get());

    if (isSystemUser) {
      // System user has access to all users home spaces
      setBuilder.addAll(allHomeSpaces.get());
    } else if (userHomeExists.get()) {
      setBuilder.add("@" + schemaConfig.getUserName());
    }

    return setBuilder.build();
  }

  @Override
  public SchemaMutability getMutability() {
    return SchemaMutability.NONE;
  }

  private SimpleSchema newTopLevelSchema(String name, SchemaType type, SimpleSchema.MetadataParam metadata, SchemaMutability schemaMutability) {
    return new SimpleSchema(sabotContext, ns, metadataStatsCollector, Collections.<String>emptyList(), name, schemaConfig, type, metadata, schemaMutability);
  }

  public void exposeSubSchemasAsTopLevelSchemas(SchemaPlus rootSchema) {
    for(String topLevelSchemaName : getSubSchemaNames()) {
      final SimpleSchema topLevelSchema = findRootLevelSubSchema(topLevelSchemaName);
      topLevelSchema.exposeSubSchemasAsTopLevelSchemas(rootSchema);
    }
  }
}
