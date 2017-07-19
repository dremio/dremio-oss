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
package com.dremio.plugins.elastic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSplitXattr;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticTableXattr;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.vector.complex.fn.WorkingBuffer;
import com.dremio.plugins.elastic.ElasticActions.Count;
import com.dremio.plugins.elastic.ElasticActions.CountResult;
import com.dremio.plugins.elastic.ElasticActions.NodesInfo;
import com.dremio.plugins.elastic.ElasticActions.Result;
import com.dremio.plugins.elastic.ElasticActions.SearchShards;
import com.dremio.plugins.elastic.ElasticConnectionPool.ElasticConnection;
import com.dremio.plugins.elastic.execution.ElasticsearchRecordReader;
import com.dremio.plugins.elastic.execution.FieldReadDefinition;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ElasticMapping;
import com.dremio.plugins.elastic.mapping.FieldAnnotation;
import com.dremio.plugins.elastic.mapping.SchemaMerger;
import com.dremio.plugins.elastic.mapping.SchemaMerger.MergeResult;
import com.dremio.plugins.elastic.planning.ElasticsearchScanSpec;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.protostuff.ByteString;

class ElasticTableBuilder implements SourceTableDefinition {
  private static final Logger logger = LoggerFactory.getLogger(ElasticTableBuilder.class);

  private static final Joiner SPLIT_KEY_JOINER = Joiner.on('-');
  private static final Joiner RESOURCE_JOINER = Joiner.on('/');

  private static final int SAMPLE_FETCH_SIZE = 100;
  private static final double SPLIT_DEFAULT_SIZE = 100000;

  private final ElasticConnection connection;
  private final NamespaceKey name;
  private final String indexOrAlias;
  private final String typeName;
  private final DatasetConfig oldConfig;
  private final BufferAllocator allocator;
  private final SabotConfig config;
  private final ElasticsearchStoragePluginConfig pluginConfig;
  private final OptionManager optionManager;
  private final ElasticMapping mapping;
  private final List<String> aliasIndices;
  private final boolean alias;

  private boolean built;
  private List<DatasetSplit> splits;
  private double count;
  private DatasetConfig dataset;

  public ElasticTableBuilder(
      ElasticConnection connection,
      NamespaceKey name,
      DatasetConfig oldConfig,
      BufferAllocator allocator,
      SabotConfig config,
      ElasticsearchStoragePluginConfig pluginConfig,
      OptionManager optionManager,
      ElasticMapping mapping,
      List<String> aliasIndices,
      boolean alias) {
    super();
    this.connection = connection;
    this.name = name;
    this.indexOrAlias = name.getPathComponents().get(1);
    this.typeName = name.getPathComponents().get(2);
    this.oldConfig = oldConfig;
    this.config = config;
    this.pluginConfig = pluginConfig;
    this.optionManager = optionManager;
    this.allocator = allocator;
    this.alias = alias;
    this.aliasIndices = aliasIndices;
    this.mapping = mapping;
  }

  @Override
  public NamespaceKey getName() {
    return name;
  }

  @Override
  public DatasetConfig getDataset() throws Exception {
    buildIfNecessary();
    return dataset;
  }

  @Override
  public boolean isSaveable() {
    return true;
  }

  @Override
  public DatasetType getType() {
    return DatasetType.PHYSICAL_DATASET;
  }

  private void buildIfNecessary() throws Exception {
    if(built){
      return;
    }

    populate();
    built = true;
  }

  private void populate() throws Exception {
    if(oldConfig != null){
      dataset = oldConfig;
    }else {
      dataset = new DatasetConfig()
          .setPhysicalDataset(new PhysicalDataset())
          .setId(new EntityId().setId(UUID.randomUUID().toString()))
          .setFullPathList(name.getPathComponents())
          .setType(DatasetType.PHYSICAL_DATASET)
          .setName(typeName);
    }

    dataset.setReadDefinition(new ReadDefinition());

    final SchemaMerger merger = new SchemaMerger();
    final BatchSchema schema;
    if(oldConfig == null || DatasetHelper.getSchemaBytes(oldConfig) == null){
      schema = null;
    } else {
      schema = BatchSchema.fromDataset(oldConfig);
    }

    MergeResult mergeResult = merger.merge(mapping, schema);

    // sample (whether we have seen stuff before or not). We always sample to improve understanding of list fields that may occur.
    BatchSchema sampledSchema = getSampledSchema(mergeResult.getSchema(), FieldAnnotation.getAnnotationMap(mergeResult.getAnnotations()));
    mergeResult = merger.merge(mapping, sampledSchema);

    dataset.setRecordSchema(ByteString.copyFrom(mergeResult.getSchema().toByteString().toByteArray()));

    final ElasticTableXattr.Builder tableAttributesB = ElasticTableXattr.newBuilder()
        .addAllAnnotation(mergeResult.getAnnotations())
        .setMappingHash(mapping.hashCode())
        .setResource(RESOURCE_JOINER.join(indexOrAlias, typeName));

    if(alias){
      String aliasFilter = getAliasFilter();
      if(aliasFilter != null){
        tableAttributesB.setAliasFilter(aliasFilter);
      }
    }

    final ElasticTableXattr tableAttributes = tableAttributesB.build();

    final ReadDefinition readDefinition = new ReadDefinition()
        .setExtendedProperty(ByteString.copyFrom(tableAttributes.toByteArray()))
        .setSplitVersion(0L);
    dataset.setReadDefinition(readDefinition);

    final List<DatasetSplit> splits = new ArrayList<>();

    NodesInfo nodesInfo = new NodesInfo();
    Result nodesResult = connection.executeAndHandleResponseCode(nodesInfo, true,
        "Cannot gather Elasticsearch nodes information. Please make sure that the user has [cluster:monitor/nodes/info] privilege.");

    JsonObject nodes = nodesResult.getAsJsonObject().getAsJsonObject("nodes");

    SearchShards searchShards = new SearchShards().addIndex(indexOrAlias);
    final Result result = connection.executeAndHandleResponseCode(searchShards, true,
        "Cannot get shards information for [" + indexOrAlias + "." + typeName + "]. Please make sure that the user has [indices:admin/shards/search_shards] privilege.");

    JsonArray shards = result.getAsJsonObject().getAsJsonArray("shards");

    Set<String> indexes = new HashSet<>();

    int s = 0;
    for (JsonElement e : shards) {
      Set<Integer> shard = new HashSet<>();
      Set<String> index = new HashSet<>();
      Set<String> hosts = new HashSet<>();
      for (JsonElement ee : e.getAsJsonArray()) {
        shard.add(ee.getAsJsonObject().get("shard").getAsInt());
        index.add(ee.getAsJsonObject().get("index").getAsString());
        String node = ee.getAsJsonObject().get("node").getAsString();
        String host = nodes.getAsJsonObject(node).get("host").getAsString();
        hosts.add(host);
      }
      Preconditions.checkArgument(shard.size() == 1, "Expected one shard, received %d.", shard.size());
      Preconditions.checkArgument(index.size() == 1, "Expected one index, received %d.", index.size());
      Preconditions.checkState(hosts.size() > 0, "No hosts found for shard:" + s);
      s++;

      final String onlyIndex = index.iterator().next();
      final int onlyShard = shard.iterator().next();

      indexes.add(onlyIndex);

      final DatasetSplit datasetSplit = new DatasetSplit()
          .setSplitKey(SPLIT_KEY_JOINER.join(onlyIndex, typeName, onlyShard))
          .setSplitVersion(0L);

      final ElasticSplitXattr splitAttributes = ElasticSplitXattr.newBuilder()
          .setResource(RESOURCE_JOINER.join(onlyIndex, typeName))
          .setShard(onlyShard)
          .build();
      datasetSplit.setExtendedProperty(ByteString.copyFrom(splitAttributes.toByteArray()));

      final List<Affinity> affinities = new ArrayList<>();
      for(String host : hosts){
        affinities.add(new Affinity().setHost(host).setFactor(SPLIT_DEFAULT_SIZE));
      }
      datasetSplit.setAffinitiesList(affinities);
      datasetSplit.setSize((long) SPLIT_DEFAULT_SIZE);
      splits.add(datasetSplit);
    }

    Count count = new Count();
    for (String index : indexes) {
      count.addIndex(index);
    }
    count.addType(typeName);
    CountResult countResult = (CountResult) connection.executeAndHandleResponseCode(count, true,
        "Cannot get the number of records in [" + indexes + "." + typeName + "].  Please make sure that the user has [read] privilege.");
    long rowCount = countResult.getAsLong();

    readDefinition.setScanStats(new ScanStats().setType(ScanStatsType.NO_EXACT_ROW_COUNT).setRecordCount(rowCount).setScanFactor(ScanCostFactor.ELASTIC.getFactor()));
    this.splits = splits;
  }

  @Override
  public List<DatasetSplit> getSplits() throws Exception {
    buildIfNecessary();
    return splits;
  }

  private BatchSchema getSampledSchema(BatchSchema schema, Map<SchemaPath, FieldAnnotation> annotations) throws Exception {
    logger.debug("Sample elastic table");

    final ElasticsearchScanSpec spec = new ElasticsearchScanSpec(
        indexOrAlias + "/" + typeName,
        null /* match all */,
        SAMPLE_FETCH_SIZE,
        false);

    try(
        BufferAllocator sampleAllocator = allocator.newChildAllocator("sample-alloc", 0, Long.MAX_VALUE);
        OperatorContextImpl operatorContext = new OperatorContextImpl(config, sampleAllocator, optionManager, SAMPLE_FETCH_SIZE);
        ){
      WorkingBuffer buffer = new WorkingBuffer(operatorContext.getManagedBuffer());
      final FieldReadDefinition readDefinition = FieldReadDefinition.getTree(schema, annotations, buffer);
      try(final ElasticsearchRecordReader reader = new ElasticsearchRecordReader(
          null,
          null,
          null,
          operatorContext,
          spec,
          false,
          null /*split*/,
          connection,
          GroupScan.ALL_COLUMNS,
          readDefinition,
          pluginConfig,
          buffer,
          schema);
      final SampleMutator mutator = new SampleMutator(allocator)
      ) {

    schema.materializeVectors(GroupScan.ALL_COLUMNS, mutator);
    reader.setup(mutator);
    reader.next();
    mutator.getContainer().buildSchema(SelectionVectorMode.NONE);
    return mutator.getContainer().getSchema();

      }
    }
  }

  private String getAliasFilter() {
    String filter = null;
    for(String alias : indexOrAlias.split(",")){
      String filterA = getAliasFilter(alias);
      if(filter == null){
        filter = filterA;
      } else if(filter.equals(filterA)){
        // the filters match.
      } else {
        //filter don't match, fail for now.
        throw UserException.validationError().message("Unable to access a collection of aliases with differing filters.").build(logger);
      }
    }

    return filter;
  }

  private String getAliasFilter(String name) {

    Result aliasResult = connection.executeAndHandleResponseCode(new ElasticActions.CatAlias(name), false, "Cannot get metadata for alias" + alias);
    if(!aliasResult.success()){
      // if we were unable to probe the alias, then it is dynamic alias (a wildcard query)
      return null;
    }

    JsonObject aliasObject = aliasResult.getAsJsonObject();
    JsonObject firstIndex = aliasObject.entrySet().iterator().next().getValue().getAsJsonObject();
    JsonObject aliasesObject = firstIndex.getAsJsonObject("aliases");
    JsonObject aliasObject2 = aliasesObject.getAsJsonObject(name);
    JsonObject filterObject = aliasObject2.getAsJsonObject("filter");
    if (filterObject == null) {
      return null;
    }
    return filterObject.toString();
  }

}
