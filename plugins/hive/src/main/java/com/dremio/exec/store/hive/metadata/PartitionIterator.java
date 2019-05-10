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
package com.dremio.exec.store.hive.metadata;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;

import com.dremio.exec.store.hive.HiveClient;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * Manages retrieval of Hive Partitions to help generate Dremio PartitionChunk objects.
 * <p>
 * A list of all partition names are retrieved in the constructor. The list is broken up into
 * batches of
 * {@link com.dremio.exec.store.hive.HivePluginOptions#HIVE_PARTITION_BATCH_SIZE_VALIDATOR}
 * elements.
 * <p>
 * Partition name batches are used to lazily load batches Hive Partition objects as the caller
 * iterates over single Hive Partition objects.
 */
public class PartitionIterator extends AbstractIterator<Partition> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionIterator.class);

  private final HiveClient client;
  private final String dbName;
  private final String tableName;

  private final Iterator<List<String>> partitionNameBatchItr;
  private Iterator<Partition> partitionBatch;

  private PartitionIterator(final HiveClient client, final String dbName, final String tableName, final int partitionBatchSize) throws TException {
    this.client = client;
    this.dbName = dbName;
    this.tableName = tableName;

    logger.trace("Database '{}', table '{}', Begin Retrieval of partition names.", dbName, tableName);

    final List<String> allPartitionNames = client.getPartitionNames(dbName, tableName);
    final List<List<String>> partitionNameBatches = Lists.partition(allPartitionNames, partitionBatchSize);

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

    public Builder partitionBatchSize(Integer partitionBatchSize) {
      this.partitionBatchSize = partitionBatchSize;
      return this;
    }

    public PartitionIterator build() throws TException {

      Objects.requireNonNull(client, "client is required");
      Objects.requireNonNull(dbName, "dbName is required");
      Objects.requireNonNull(tableName, "tableName is required");
      Objects.requireNonNull(partitionBatchSize, "partitionBatchSize is required");

      return new PartitionIterator(client, dbName, tableName, partitionBatchSize);
    }
  }
}
