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

import org.apache.hadoop.fs.Path;

public class WritePartition {

  public static WritePartition NONE = new WritePartition(null);

  private final String[] paths;
  private final Integer distributionOrdinal;

  public WritePartition(String[] paths){
    this.paths = paths;
    this.distributionOrdinal = null;
  }

  public WritePartition(String[] paths, Integer distributionOrdinal) {
    super();
    this.paths = paths;
    this.distributionOrdinal = distributionOrdinal;
  }

  public boolean isSinglePartition(){
    return paths == null;
  }

  public Integer getBucketNumber(){
    return distributionOrdinal;
  }

  public Path qualified(String baseLocation, String name){
    return qualified(new Path(baseLocation), name);
  }

  public Path qualified(Path baseLocation, String name){
    if(paths == null){
      return new Path(baseLocation, name);
    }

    Path path = baseLocation;

    if(distributionOrdinal != null){
      path = new Path(path, Integer.toString(distributionOrdinal));
    }

    for(String partition : paths){
      path = new Path(path, partition);
    }

    return new Path(path, name);
  }
}
