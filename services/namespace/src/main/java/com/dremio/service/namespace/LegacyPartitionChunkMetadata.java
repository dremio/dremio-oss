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
package com.dremio.service.namespace;

import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Representation for partition chunks that were created before the separation of dataset splits into MultiSplit
 * It will also be used, in the short term, while the code base is moving to a full use of the metadata API
 */
@Deprecated
public class LegacyPartitionChunkMetadata extends AbstractPartitionChunkMetadata {

  public LegacyPartitionChunkMetadata(PartitionChunk partitionChunk) {
    super(partitionChunk);
    Preconditions.checkArgument(!partitionChunk.hasSplitCount(), "Must be constructed with a legacy partitionChunk");
  }

  @Override
  public Iterable<DatasetSplit> getDatasetSplits() {
    PartitionChunk partitionChunk = getPartitionChunk();

    DatasetSplit split = DatasetSplit.newBuilder()
        .addAllAffinities(partitionChunk.getAffinitiesList())
        .setSize(partitionChunk.getSize())
        // N.B. "legacy" consumers of DatasetSplit expected extended property as part of split extended info, but now
        // the "split" abstraction is called "partition chunk", so get the extended property from partition chunk.
        // So "split" in this context, is different from split in multi-split. Or simply put, you should rather delete
        // this class :)
        .setSplitExtendedProperty(partitionChunk.getPartitionExtendedProperty())
        .build();
    return ImmutableList.of(split);
  }

  @Override
  public int getSplitCount() {
    // each legacy partition chunk represent(ed) a single split
    return 1;
  }

  @Override
  public boolean checkPartitionChunkMetadataConsistency() { return true; }

}
