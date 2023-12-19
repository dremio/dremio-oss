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
package com.dremio.exec.store.iceberg;

import org.apache.iceberg.transforms.PartitionSpecVisitor;

/**
 * Collects the bucketCount or truncateLength from Bucket and Truncate transforms
 * It will throw UnsupportedOperationException if called on a field with any other transform
 */
public final class CollectArgsPartitionSpecVisitor implements PartitionSpecVisitor<Integer> {
  CollectArgsPartitionSpecVisitor() {
  }

  @Override
  public Integer truncate(final String sourceName,final int sourceId,final int width) {
    return width;
  }

  @Override
  public Integer bucket(final String sourceName,final int sourceId,final int numBuckets) {
    return numBuckets;
  }
}
