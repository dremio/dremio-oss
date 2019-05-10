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

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSplitXattr;
import com.dremio.plugins.elastic.ElasticActions.Count;
import com.dremio.plugins.elastic.ElasticActions.CountResult;
import com.dremio.plugins.elastic.ElasticActions.NodesInfo;
import com.dremio.plugins.elastic.ElasticActions.Result;
import com.dremio.plugins.elastic.ElasticActions.SearchShards;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 *
 * Representation of PartitionChunkListing for the elastic plugin.
 *
 * Contains methods to build a list of partition chunks if it doesn't
 * already exist.
 *
 */
class ElasticPartitionChunkListing implements PartitionChunkListing {
  private static final Joiner RESOURCE_JOINER = Joiner.on('/');

  private static final double SPLIT_DEFAULT_SIZE = 100000;

  private final ElasticDatasetHandle datasetHandle;
  private final String indexOrAlias;
  private final String typeName;

  private long rowCount;
  private List<PartitionChunk> partitionChunkList;

  public ElasticPartitionChunkListing(ElasticDatasetHandle datasetHandle) {
    super();
    this.datasetHandle = datasetHandle;

    this.indexOrAlias = datasetHandle.getDatasetPath().getComponents().get(1);
    //encode the typeName incase it has a slash or other special characters
    String temp = datasetHandle.getDatasetPath().getComponents().get(2);
    try {
      temp = URLEncoder.encode(datasetHandle.getDatasetPath().getComponents().get(2), "UTF-8");
    } catch(Exception ignore) {
    }
    this.typeName = temp;
  }

  String getIndexOrAlias() {
    return indexOrAlias;
  }

  String getTypeName() {
    return typeName;
  }

  long getRowCount() {
    return rowCount;
  }

  @Override
  public Iterator<? extends PartitionChunk> iterator() {
    if (partitionChunkList != null) {
      return partitionChunkList.iterator();
    }

    final List<PartitionChunk> partitionChunks = new ArrayList<>();

    NodesInfo nodesInfo = new NodesInfo();
    Result nodesResult = datasetHandle.getConnection().executeAndHandleResponseCode(nodesInfo, true,
      "Cannot gather Elasticsearch nodes information. Please make sure that the user has [cluster:monitor/nodes/info] privilege.");

    JsonObject nodes = nodesResult.getAsJsonObject().getAsJsonObject("nodes");

    SearchShards searchShards = new SearchShards().addIndex(indexOrAlias);
    final Result result = datasetHandle.getConnection().executeAndHandleResponseCode(searchShards, true,
      "Cannot get shards information for [" + indexOrAlias + "." + typeName + "]. Please make sure that the user has [indices:admin/shards/search_shards] privilege.");

    JsonArray shards = result.getAsJsonObject().getAsJsonArray("shards");

    Set<String> indexes = new HashSet<>();

    for (JsonElement e : shards) {
      Set<Integer> shard = new HashSet<>();
      Set<String> index = new HashSet<>();
      Set<String> hosts = new HashSet<>();
      for (JsonElement ee : e.getAsJsonArray()) {
        shard.add(ee.getAsJsonObject().get("shard").getAsInt());
        index.add(ee.getAsJsonObject().get("index").getAsString());
        String node = ee.getAsJsonObject().get("node").getAsString();
        final JsonElement host = nodes.getAsJsonObject(node).get("host");
        if (host != null) {
          hosts.add(host.getAsString());
        }
      }
      Preconditions.checkArgument(shard.size() == 1, "Expected one shard, received %d.", shard.size());
      Preconditions.checkArgument(index.size() == 1, "Expected one index, received %d.", index.size());

      final String onlyIndex = index.iterator().next();
      final int onlyShard = shard.iterator().next();

      indexes.add(onlyIndex);

      final ElasticSplitXattr splitAttributes = ElasticSplitXattr.newBuilder()
        .setResource(RESOURCE_JOINER.join(onlyIndex, typeName))
        .setShard(onlyShard)
        .build();

      List<DatasetSplitAffinity> affinity = new ArrayList<>();
      for (String host : hosts) {
        affinity.add(DatasetSplitAffinity.of(host, SPLIT_DEFAULT_SIZE));
      }

      partitionChunks.add(PartitionChunk.of(
        DatasetSplit.of(affinity, (long) SPLIT_DEFAULT_SIZE, 0, os -> splitAttributes.writeTo(os))));

      partitionChunkList = partitionChunks;
    }

    Count count = new Count();
    for (String index : indexes) {
      count.addIndex(index);
    }
    count.addType(typeName);
    CountResult countResult = (CountResult) datasetHandle.getConnection().executeAndHandleResponseCode(count, true,
      "Cannot get the number of records in [" + indexes + "." + typeName + "].  Please make sure that the user has [read] privilege.");

    rowCount = countResult.getAsLong();

    return partitionChunks.iterator();
  }
}
