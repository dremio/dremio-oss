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
package com.dremio.exec.store.hive.metadata;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Partition;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.hive.HiveClient;
import com.dremio.hive.thrift.TException;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * Manages retrieval of Hive Partitions to help generate Dremio PartitionChunk objects.
 * <p>
 * A list of all partition names are retrieved in the constructor. The list is broken up into
 * batches of HIVE_PARTITION_BATCH_SIZE_VALIDATOR elements.
 * <p>
 * Partition name batches are used to lazily load batches Hive Partition objects as the caller
 * iterates over single Hive Partition objects.
 */
public final class PartitionIterator extends AbstractIterator<Partition> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionIterator.class);

  private final HiveClient client;
  private final String dbName;
  private final String tableName;

  private final Iterator<List<String>> partitionNameBatchItr;
  private Iterator<Partition> partitionBatch;

  private PartitionIterator(final HiveClient client, final String dbName, final String tableName, final List<String> filteredPartitionNames, final int partitionBatchSize) throws TException {
    this.client = client;
    this.dbName = dbName;
    this.tableName = tableName;

    logger.trace("Database '{}', table '{}', Begin Retrieval of partition names.", dbName, tableName);

    List<String> allPartitionNames = client.getPartitionNames(dbName, tableName);
    final List<List<String>> partitionNameBatches = Lists.partition(filterPartitions(filteredPartitionNames, allPartitionNames, dbName, tableName), partitionBatchSize);

    partitionNameBatchItr = partitionNameBatches.iterator();

    logger.debug("Database '{}', table '{}', {} partition names split into {} batches with batch size: {}",
      dbName, tableName, allPartitionNames.size(), partitionNameBatches.size(), partitionBatchSize);

    // prime the iterator.
    if (partitionNameBatchItr.hasNext()) {
      partitionBatch = client.getPartitionsByName(dbName, tableName, partitionNameBatchItr.next()).iterator();
    } else {
      partitionBatch = Collections.emptyIterator();
    }
  }

  @Override
  protected Partition computeNext() {
    try {
      if (!partitionBatch.hasNext() && partitionNameBatchItr.hasNext()) {
        List<String> partitionNameBatch = partitionNameBatchItr.next();
        partitionBatch = client.getPartitionsByName(dbName, tableName, partitionNameBatch).iterator();
      }

      if (partitionBatch.hasNext()) {
        Partition partition = partitionBatch.next();
        return partition;
      } else {
        return endOfData();
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }


  public static final class Builder {
    private HiveClient client;
    private String dbName;
    private String tableName;
    private List<String> filteredPartitionNames;
    private Integer partitionBatchSize;

    public Builder client(HiveClient client) {
      this.client = client;
      return this;
    }

    public Builder dbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder filteredPartitionNames(List<String> filteredPartitionNames) {
      this.filteredPartitionNames = filteredPartitionNames;
      return this;
    }

    public Builder partitionBatchSize(Integer partitionBatchSize) {
      this.partitionBatchSize = partitionBatchSize;
      return this;
    }

    public PartitionIterator build() throws TException {

      Objects.requireNonNull(client, "client is required");
      Objects.requireNonNull(dbName, "dbName is required");
      Objects.requireNonNull(tableName, "tableName is required");
      Objects.requireNonNull(partitionBatchSize, "partitionBatchSize is required");

      return new PartitionIterator(client, dbName, tableName, filteredPartitionNames, partitionBatchSize);
    }
  }

  private List<String> filterPartitions(final List<String> filteredPartitionNames, final List<String> allPartitionNames, String dbName, String tableName) {
    if(filteredPartitionNames == null || filteredPartitionNames.isEmpty()) {
      return allPartitionNames;
    }

    Preconditions.checkArgument(filteredPartitionNames.size() == 1);

    Map<String, String> originalToDecoded = allPartitionNames.stream().collect(Collectors.toMap(x -> x, x -> URLDecoder.decode(x, StandardCharsets.UTF_8)));

    List<String> matchedPartitions = new ArrayList<>();
    List<String> matchedPartitionsDecoded = new ArrayList<>();

    for (Entry<String, String> entry : originalToDecoded.entrySet()) {
      if (filteredPartitionNames.contains(entry.getValue())) {
        matchedPartitions.add(entry.getKey());
        matchedPartitionsDecoded.add(entry.getValue());
      }
    }

    if (matchedPartitions.size() != filteredPartitionNames.size()) {
      //We haven't found match for few filteredPartitionNames
      filteredPartitionNames.removeAll(matchedPartitionsDecoded);
      String nonMatchPar = filteredPartitionNames.stream().collect(Collectors.joining(","));
      String fullTableName = dbName + "." + tableName;
      throw UserException
              .validationError()
              .message("Partition '%s' does not exist in %s", nonMatchPar, fullTableName)
              .buildSilently();
    }

    // This condition is for safe check. will occur only in case originalToDecoded is empty
    Preconditions.checkArgument(!matchedPartitions.isEmpty(), "Supplied partitions doesn't exist in Hive");
    return matchedPartitions;
  }
}
