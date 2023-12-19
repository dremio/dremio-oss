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
package com.dremio.exec.store;

import static java.lang.String.format;

import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.io.file.Path;

public class WritePartition {

  public static WritePartition NONE = new WritePartition(null);

  private final String[] partitionValues;
  private final Integer offset;
  private final Integer distributionOrdinal;
  private final IcebergPartitionData icebergPartitionData;

  public WritePartition(String[] partitionValues) {
    this(partitionValues, null, null, null);
  }

  public WritePartition(String[] partitionValues, Integer offset, Integer distributionOrdinal) {
    this(partitionValues, offset, distributionOrdinal, null);
  }

  public WritePartition(String[] partitionValues, Integer offset, Integer distributionOrdinal, IcebergPartitionData icebergPartitionData) {
    super();
    this.partitionValues = partitionValues;
    this.offset = offset;
    this.distributionOrdinal = distributionOrdinal;
    this.icebergPartitionData = icebergPartitionData;
  }

  public boolean isSinglePartition(){
    return partitionValues== null;
  }

  public Integer getBucketNumber(){
    return distributionOrdinal;
  }

  public Path getQualifiedPath(String baseLocation, String name){
    return getQualifiedPath(Path.of(baseLocation), name);
  }

  public Path getQualifiedPath(Path baseLocation, String name){
    if(partitionValues== null){
      return baseLocation.resolve(name);
    }

    Path path = baseLocation;

    if(distributionOrdinal != null){
      path = path.resolve(Integer.toString(distributionOrdinal));
    }

    for(String partition : partitionValues){
      String partitionValuePath = format("%s_%s", offset, partition);
      path = path.resolve(partitionValuePath);
    }

    return path.resolve(name);
  }

  public IcebergPartitionData getIcebergPartitionData() {
    return icebergPartitionData;
  }

  public String getPartitionValues() {
    if(partitionValues== null){
      return null;
    }
    return String.join("_", partitionValues);
  }
}
