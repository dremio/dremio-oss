/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.plugins.elastic;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.plugins.elastic.ElasticActions.Health;
import com.dremio.plugins.elastic.ElasticActions.IndexExists;
import com.dremio.plugins.elastic.ElasticActions.Result;
import com.dremio.plugins.elastic.ElasticConnectionPool.ElasticConnection;
import com.dremio.plugins.elastic.ElasticConnectionPool.TLSValidationMode;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ClusterMetadata;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ElasticIndex;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ElasticMapping;
import com.dremio.plugins.elastic.planning.ElasticRulesFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.BooleanCapability;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.protostuff.ByteString;

/**
 * Storage plugin for elasticsearch.
 *
 * If using Shield, we need the following privileges:
 *
 * # dremio user
 * dremio_user:
 * cluster:
 * - cluster:monitor/nodes/info
 * - cluster:monitor/state
 * - cluster:monitor/health
 * indices:
 * 'twitter' :
 * - read
 * - indices:monitor/stats
 * - indices:admin/mappings/get
 * - indices:admin/shards/search_shards
 *
 */
public class ElasticsearchStoragePlugin implements StoragePlugin {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchStoragePlugin.class);

  public static final BooleanCapability ENABLE_V5_FEATURES = new BooleanCapability("enable_elastic_v5_feature", false);
  public static final BooleanCapability SUPPORTS_NEW_FEATURES = new BooleanCapability("supports_new_features", false);

  private final String name;
  private final SabotContext context;
  private final ElasticStoragePluginConfig config;
  private final ElasticConnectionPool connectionPool;

  public ElasticsearchStoragePlugin(ElasticStoragePluginConfig config, SabotContext context, String name) {
    this.config = config;
    this.context = context;
    this.name = name;

    final TLSValidationMode tlsMode;
    if (!config.sslEnabled) {
      tlsMode = TLSValidationMode.OFF;
    } else {
      // If encryption is enabled, but validation is not set (upgrade?),
      // assume the highest level of security.
      final EncryptionValidationMode encryptionValidationMode = Optional.ofNullable(config.encryptionValidationMode)
          .orElse(EncryptionValidationMode.CERTIFICATE_AND_HOSTNAME_VALIDATION);
      switch(encryptionValidationMode) {
      case CERTIFICATE_AND_HOSTNAME_VALIDATION:
        tlsMode = TLSValidationMode.STRICT;
        break;
      case CERTIFICATE_ONLY_VALIDATION:
        tlsMode = TLSValidationMode.VERIFY_CA;
        break;
      case NO_VALIDATION:
        tlsMode = TLSValidationMode.UNSECURE;
        break;
      default:
        // Should not happen since enum is in the config class
        throw new AssertionError("Unknown encryption validation mode " + encryptionValidationMode);
      }
    }

    this.connectionPool = new ElasticConnectionPool(
        config.hostList,
        tlsMode,
        config.username,
        config.password,
        config.readTimeoutMillis,
        config.useWhitelist);
  }

  public SabotContext getContext() {
    return context;
  }

  public ElasticStoragePluginConfig getConfig(){
    return config;
  }

  ElasticConnection getRandomConnection(){
    return connectionPool.getRandomConnection();
  }

  public ElasticConnection getConnection(Iterable<String> hostsIter){
    List<String> hosts = ImmutableList.copyOf(hostsIter);
    Set<String> hostSet = ImmutableSet.copyOf(hosts);
    if(hosts.isEmpty()){
      return getRandomConnection();
    }else{
      final String localAddress = context.getEndpoint().getAddress();
      // If there is a local elastic, use that.
      if(hostSet.contains(localAddress)){
        return connectionPool.getConnection(ImmutableList.of(localAddress));
      }
      return connectionPool.getConnection(hosts);
    }
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return connectionPool.getCapabilities();
  }

  @Override
  public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldConfig, DatasetRetrievalOptions ignored) throws Exception {
    return getDatasetInternal(datasetPath, oldConfig);
  }

  private SourceTableDefinition getDatasetInternal(NamespaceKey datasetPath, DatasetConfig oldConfig) throws Exception {
    if(datasetPath.size() != 3){
      return null;
    }

    final ElasticConnection connection = this.connectionPool.getRandomConnection();
    try {
      final String schema = datasetPath.getPathComponents().get(1);
      final String type = datasetPath.getPathComponents().get(2);
      ClusterMetadata clusterMetadata = connection.execute(new ElasticActions.GetClusterMetadata().setIndex(datasetPath.getPathComponents().get(1)));
      List<ElasticIndex> indices = clusterMetadata.getIndices();
      if(indices.isEmpty()){
        return null;
      }

      final ElasticIndex firstIndex = indices.get(0);
      if(firstIndex.getName().equals(schema)){
        // not an alias.
        ElasticIndex filteredIndex = firstIndex.filterToType(type);
        if(filteredIndex == null){
          // no type for this path.
          return null;
        }
        Preconditions.checkArgument(indices.size() == 1, "More than one Index returned for alias %s.", schema);
        logger.debug("Found mapping: {} for {}:{}", filteredIndex.getMergedMapping(), schema, type);
        return new ElasticTableBuilder(connection, datasetPath, oldConfig, context.getAllocator(), context.getConfig(), config, context.getOptionManager(), filteredIndex.getMergedMapping(), ImmutableList.<String>of(), false);
      } else {

        ElasticMappingSet ems = new ElasticMappingSet(indices).filterToType(type);
        if(ems.isEmpty()){
          return null;
        }
        ElasticMapping mapping = ems.getMergedMapping();
        final List<String> indicesList = FluentIterable.from(indices).transform(new Function<ElasticIndex, String>(){
          @Override
          public String apply(ElasticIndex input) {
            return input.getName();
          }}).toList();

        logger.debug("Found mapping: {} for {}:{}", mapping, schema, type);
        return new ElasticTableBuilder(connection, datasetPath, oldConfig, context.getAllocator(), context.getConfig(), config, context.getOptionManager(), mapping, indicesList, true);
      }


    } catch (Exception ex){
      logger.info("Failure while attempting to retrieve dataset {}", datasetPath, ex);
    }

    // failure or not found.
    return null;
  }

  public ElasticMapping getMapping(NamespaceKey datasetPath){
    if(datasetPath.size() != 3){
      return null;
    }

    final ElasticConnection connection = this.connectionPool.getRandomConnection();
    try {
      final String schema = datasetPath.getPathComponents().get(1);
      final String type = datasetPath.getPathComponents().get(2);
      ClusterMetadata clusterMetadata = connection.execute(new ElasticActions.GetClusterMetadata().setIndex(datasetPath.getPathComponents().get(1)));
      List<ElasticIndex> indices = clusterMetadata.getIndices();
      if(indices.isEmpty()){
        return null;
      }

      final ElasticIndex firstIndex = indices.get(0);
      if(firstIndex.getName().equals(schema)){
        // not an alias.
        ElasticIndex index = firstIndex.filterToType(type);
        if(index == null){
          // no type for this path.
          return null;
        }
        Preconditions.checkArgument(indices.size() == 1, "More than one Index returned for alias %s.", schema);
        return index.getMappings().get(0);
      } else {

        ElasticMappingSet ems = new ElasticMappingSet(indices).filterToType(type);
        if(ems.isEmpty()){
          return null;
        }
        return ems.getMergedMapping();
      }
    } catch (Exception ex){
      logger.info("Failure while attempting to retrieve dataset {}", datasetPath, ex);
      return null;
    }
  }

  @Override
  public SourceState getState() {
    try {
      final Result result = (connectionPool.getRandomConnection()
          .executeAndHandleResponseCode(new Health(), true, "Cannot get cluster health information.  Please make sure that the user has [cluster:monitor/health] privilege."));
      if (result.success()) {
        String clusterHealth = result.getAsJsonObject().get("status").getAsString();
        switch (clusterHealth) {
          case "green":
            if (connectionPool.getCapabilities().getCapability(SUPPORTS_NEW_FEATURES)) {
              return SourceState.goodState(String.format("Elastic version %s.",
                  connectionPool.getMinVersionInCluster()));
            } else {
              return SourceState.warnState(
                  String.format("Detected Elastic version %s. Full query pushdown in Dremio requires version %s or above.",
                      connectionPool.getMinVersionInCluster(),
                      ElasticConnectionPool.MIN_VERSION_TO_ENABLE_NEW_FEATURES));
            }
          case "yellow":
            return SourceState.warnState("Elastic cluster health is yellow.");
          case "red":
            return SourceState.badState("Elastic cluster health is red.");
          default:
            return SourceState.badState(String.format("Elastic cluster health is unknown health state of %s.", clusterHealth));
        }
      } else {
        return SourceState.badState("Failure getting Elastic health. " + result.getErrorMessage());
      }
    } catch (Exception e) {
      return SourceState.badState("Failure getting Elastic health. " + e.getMessage());
    }
  }

  private static class ElasticAliasMappingName {
    private final String alias;
    private final String mapping;

    public ElasticAliasMappingName(String alias, String mapping) {
      super();
      this.alias = alias;
      this.mapping = mapping;
    }
    public String getAlias() {
      return alias;
    }
    public String getMapping() {
      return mapping;
    }
    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof ElasticAliasMappingName)) {
        return false;
      }
      ElasticAliasMappingName castOther = (ElasticAliasMappingName) other;
      return Objects.equal(alias, castOther.alias) && Objects.equal(mapping, castOther.mapping);
    }
    @Override
    public int hashCode() {
      return Objects.hashCode(alias, mapping);
    }
  }

  @Override
  public Iterable<SourceTableDefinition> getDatasets(String user, DatasetRetrievalOptions ignored) throws Exception {
    final ElasticConnection connection = this.connectionPool.getRandomConnection();
    final ClusterMetadata clusterMetadata = connection.execute(new ElasticActions.GetClusterMetadata());
    final ImmutableList.Builder<SourceTableDefinition> builder = ImmutableList.builder();
    boolean failures = false;

    ArrayListMultimap<ElasticAliasMappingName, ElasticIndex> aliases = ArrayListMultimap.create();
    final boolean includeHiddenSchemas = config.showHiddenIndices;
        for(ElasticIndex index : clusterMetadata.getIndices()){
      for(ElasticMapping mapping : index.getMappings()){
        try {
          if(includeHiddenSchemas || !index.getName().startsWith(".")){
            NamespaceKey key = new NamespaceKey(ImmutableList.of(name, index.getName(), mapping.getName()));
            builder.add(new ElasticTableBuilder(connection, key, null, context.getAllocator(), context.getConfig(), config, context.getOptionManager(), mapping, ImmutableList.<String>of(), false));
          }
          for(String alias : index.getAliases()){
            aliases.put(new ElasticAliasMappingName(alias, mapping.getName()), new ElasticIndex(index.getName(), mapping));
          }
        } catch (Exception ex){
          logger.info("Failure to read information for {}.{}", index.getName(), mapping.getName(), ex);
        }
      }
    }

    for(ElasticAliasMappingName alias : aliases.keySet()){
      List<ElasticIndex> indices = aliases.get(alias);
      final List<String> indicesList = FluentIterable.from(indices).transform(new Function<ElasticIndex, String>(){
        @Override
        public String apply(ElasticIndex input) {
          return input.getName();
        }}).toList();


      ElasticMappingSet mappingSet = new ElasticMappingSet(indices);

      try{

        ElasticMapping mapping = mappingSet.getMergedMapping();
        NamespaceKey key = new NamespaceKey(ImmutableList.of(name, alias.getAlias(), mapping.getName()));
        builder.add(new ElasticTableBuilder(connection, key, null, context.getAllocator(), context.getConfig(), config, context.getOptionManager(), mapping, indicesList, true));
      }catch(Exception ex){
        logger.info("Failure to read schema information for alias {}", alias, ex);
      }
    }

    List<SourceTableDefinition> datasets = builder.build();
    if (datasets.isEmpty()) {
      logger.debug("No indices/types available. Please make sure to populate the cluster");
      if (failures) {
        throw UserException.dataReadError().message("Could not find any accessible indices/types querying Elasticsearch metadata. "
            + "Please make sure that the user has [indices:admin/get] privilege.").build(logger);
      }
    }
    return datasets;
  }

  @Override
  public boolean containerExists(NamespaceKey key) {

    if (key.size() != 2) {
      return false;
    }

    String schema = key.getPathComponents().get(1);

    try{
      IndexExists exists = new IndexExists();
      exists.addIndex(schema);
      return getRandomConnection().executeAndHandleResponseCode(exists, false, "").success();
    } catch (Exception e) {
      logger.warn("Failure while evaluating if index or alias '{}' exists.", key, e);
      return false;
    }

  }

  @Override
  public boolean datasetExists(NamespaceKey key) {
    if (key.size() != 3) {
      return false;
    }

    try{
      return getDataset(key, null, DatasetRetrievalOptions.IGNORE_AUTHZ_ERRORS) != null;
    } catch (Exception e) {
      logger.warn("Failure while evaluating if dataset '{}' exists.", key, e);
      return false;
    }
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return context.getConfig().getClass("dremio.plugins.elastic.rulesfactory", StoragePluginRulesFactory.class, ElasticRulesFactory.class);
  }

  @Override
  public CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig, DatasetRetrievalOptions ignored) throws Exception {
    NamespaceKey namespaceKey = new NamespaceKey(datasetConfig.getFullPathList());
    final SourceTableDefinition definition = getDatasetInternal(namespaceKey, datasetConfig);

    if(definition == null){
      return CheckResult.DELETED;
    }

    return new CheckResult(){

      @Override
      public UpdateStatus getStatus() {
        return UpdateStatus.CHANGED;
      }

      @Override
      public SourceTableDefinition getDataset() {
        return definition;
      }};
  }

  @Override
  public void start() throws IOException {
    connectionPool.connect();
  }

  @Override
  public void close() throws Exception {
    logger.debug("Closing elasticsearch storage plugin");
    AutoCloseables.close(connectionPool);
  }
}
