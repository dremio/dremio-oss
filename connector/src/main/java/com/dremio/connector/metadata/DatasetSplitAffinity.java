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
package com.dremio.connector.metadata;

/**
 * Interface for a connector to provide details of affinity related to a split.
 */
public interface DatasetSplitAffinity {

  /**
   * Get the host.
   *
   * @return the host, not null
   */
  String getHost();

  /**
   * Get the factor.
   *
   * @return the factor
   * TODO: what does this mean, similar to {@link DatasetStats#getScanFactor}?
   */
  double getFactor();

  /**
   * Create {@code DatasetSplitAffinity}.
   *
   * @param host hostname
   * @param factor scan factor
   * @return dataset split affinity
   */
  static DatasetSplitAffinity of(String host, double factor) {
    return new DatasetSplitAffinity() {

      @Override
      public String getHost() {
        return host;
      }

      @Override
      public double getFactor() {
        return factor;
      }
    };
  }
}
