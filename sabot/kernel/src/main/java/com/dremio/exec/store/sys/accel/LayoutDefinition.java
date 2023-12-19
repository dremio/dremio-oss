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
package com.dremio.exec.store.sys.accel;

import java.util.List;

import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.NameAndGranularity;
import com.dremio.exec.planner.sql.parser.SqlCreateReflection.NameAndMeasures;

public class LayoutDefinition {

  public static enum Type {AGGREGATE, RAW};

  private final Type type;
  private final List<String> display;
  private final List<NameAndGranularity> dimension;
  private final List<NameAndMeasures> measure;
  private final List<String> sort;
  private final List<String> distribution;
  private final List<PartitionTransform> partition;
  private final Boolean arrowCachingEnabled;
  private final PartitionDistributionStrategy partitionDistributionStrategy;
  private final String name;

  public LayoutDefinition(
    String name, Type type,
    List<String> display,
    List<NameAndGranularity> dimension,
    List<NameAndMeasures> measure,
    List<String> sort,
    List<String> distribution,
    List<PartitionTransform> partition,
    Boolean arrowCachingEnabled,
    PartitionDistributionStrategy partitionDistributionStrategy) {
    super();
    this.name = name;
    this.type = type;
    this.display = display;
    this.dimension = dimension;
    this.measure = measure;
    this.sort = sort;
    this.distribution = distribution;
    this.partition = partition;
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.partitionDistributionStrategy = partitionDistributionStrategy;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }


  public List<String> getDisplay() {
    return display;
  }


  public List<NameAndGranularity> getDimension() {
    return dimension;
  }


  public List<NameAndMeasures> getMeasure() {
    return measure;
  }


  public List<String> getSort() {
    return sort;
  }


  public List<String> getDistribution() {
    return distribution;
  }


  public List<PartitionTransform> getPartition() {
    return partition;
  }

  public Boolean getArrowCachingEnabled() { return arrowCachingEnabled; }

  public PartitionDistributionStrategy getPartitionDistributionStrategy() {
    return partitionDistributionStrategy;
  }

}
