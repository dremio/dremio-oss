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
import com.dremio.exec.store.SchemaTreeProvider.MetadataStatsCollector;
import com.dremio.exec.store.SchemaTreeProvider.SchemaType;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class RootSchema extends AbstractSchema {

  private final NamespaceService ns;
  private final CatalogService catalog;
  private final SchemaConfig schemaConfig;
  private final SabotContext sabotContext;
  private final Supplier<Map<String, SourceConfig>> sources;
  private final Supplier<Map<String, String>> spaces;
  private final Supplier<Map<String, String>> allHomeSpaces;
  private final Supplier<Boolean> userHomeExists;
  private final MetadataStatsCollector metadataStatsCollector;
  private final boolean isSystemUser;

  public RootSchema(final NamespaceService ns, final CatalogService catalog, final SabotContext sabotContext, final SchemaConfig schemaConfig,
                    final MetadataStatsCollector metadataStatsCollector) {
    super(Collections.<String>emptyList(), "");
    this.ns = ns;
    this.catalog = catalog;
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
            builder.put(sourceName.toLowerCase(), sourceConfig);
          }
        }
        return builder.build();
      }
    });
    this.spaces = Suppliers.memoize(new Supplier<Map<String, String>>() {
      @Override
      public Map<String, String> get() {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for(final SpaceConfig spaceConfig : ns.getSpaces()) {
          builder.put(spaceConfig.getName().toLowerCase(), spaceConfig.getName());
        }
        return builder.build();
      }
    });
    this.allHomeSpaces = Suppliers.memoize(new Supplier<Map<String, String>>() {
      @Override
      public Map<String, String> get() {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for(final HomeConfig homeConfig : ns.getHomeSpaces()) {
          builder.put("@" + homeConfig.getOwner().toLowerCase(), "@" + homeConfig.getOwner());
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
    String nameLC = name.toLowerCase();
    if (sources.get().containsKey(nameLC)) {
      StoragePlugin plugin = null;
      try{
        plugin = sabotContext.getStorage().getPlugin(name);
        if(plugin == null) {
          plugin = sabotContext.getStorage().getPlugin(nameLC);
        }
      }catch(ExecutionSetupException e){
        logger.info("Failure while attempting to retrieve plugin for name {}.", name, e);
      }
      SourceConfig config = sources.get().get(nameLC);
      MetadataPolicy policy = config.getMetadataPolicy() != null ? config.getMetadataPolicy() : CatalogService.DEFAULT_METADATA_POLICY;
      SimpleSchema.MetadataParam metadataParam = new SimpleSchema.MetadataParam(policy, catalog.getLastFullMetadataRefreshDateMs(new NamespaceKey(nameLC)));

      final SchemaMutability mutability;
      if(plugin != null && plugin instanceof FileSystemPlugin) {
        mutability = ((FileSystemPlugin) plugin).getMutability();
      } else {
        mutability = SchemaMutability.NONE;
      }
      return newTopLevelSchema(name, SchemaType.SOURCE, metadataParam, mutability);
    }

    if (spaces.get().containsKey(nameLC)) {
      return newTopLevelSchema(name, SchemaType.SPACE, null, SchemaMutability.USER_VIEW);
    }

    if (isSystemUser && allHomeSpaces.get().containsKey(nameLC)) {
      return newTopLevelSchema(name, SchemaType.HOME, null, SchemaMutability.USER_VIEW);
    }

    if (userHomeExists.get()) {
      final String homeName = "@" + schemaConfig.getUserName();
      if (homeName.equalsIgnoreCase(name)) {
        return newTopLevelSchema(homeName, SchemaType.HOME, null, SchemaMutability.USER_VIEW);
      }
    }

    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    // Convert this into an iterator
    final ImmutableSet.Builder<String> setBuilder = ImmutableSet.<String>builder()
        .addAll(FluentIterable.from(sources.get().values()).transform(new Function<SourceConfig, String>() {
          @Override
          public String apply(SourceConfig input) {
            return input.getName();
          }}))
        .addAll(spaces.get().values());

    if (isSystemUser) {
      // System user has access to all users home spaces
      setBuilder.addAll(allHomeSpaces.get().values());
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
