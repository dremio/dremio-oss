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
package com.dremio.service.jobtelemetry.server;

import java.util.stream.Stream;

import com.dremio.exec.proto.CoordExecRPC.QueryProgressMetrics;

/**
 * Combiner for all individual executor metrics.
 */
final class MetricsCombiner {
  private final Stream<QueryProgressMetrics> executorMetrics;

  private MetricsCombiner(Stream<QueryProgressMetrics> executorMetrics) {
    this.executorMetrics = executorMetrics;
  }

  private QueryProgressMetrics combine() {
    long rowsProcessed =
        executorMetrics
          .mapToLong(QueryProgressMetrics::getRowsProcessed)
          .sum();

    return QueryProgressMetrics.newBuilder()
      .setRowsProcessed(rowsProcessed)
      .build();
  }

  static QueryProgressMetrics combine(Stream<QueryProgressMetrics> executorMetrics) {
    return new MetricsCombiner(executorMetrics).combine();
  }
}
