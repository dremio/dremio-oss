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
package com.dremio.services.nodemetrics;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.MemoryIterator;
import com.dremio.exec.store.sys.ThreadsIterator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides metrics about the running coordinators. */
public class CoordinatorMetricsService
    extends CoordinatorMetricsServiceGrpc.CoordinatorMetricsServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(CoordinatorMetricsService.class);

  private final Provider<SabotContext> contextProvider;

  public CoordinatorMetricsService(Provider<SabotContext> contextProvider) {
    this.contextProvider = contextProvider;
  }

  @Override
  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  public void getMetrics(
      CoordinatorMetricsProto.GetMetricsRequest request,
      StreamObserver<CoordinatorMetricsProto.GetMetricsResponse> responseObserver) {
    try {
      final ThreadsIterator threads = new ThreadsIterator(contextProvider.get());
      double cpuUtilization = calculateCpuUtilization(threads);

      MemoryIterator.MemoryInfo memoryInfo =
          (MemoryIterator.MemoryInfo) new MemoryIterator(contextProvider.get()).next();
      long heapMax = memoryInfo.heap_max;
      long heapCurrent = memoryInfo.heap_current;

      CoordinatorMetricsProto.CoordinatorMetrics.Builder builder =
          CoordinatorMetricsProto.CoordinatorMetrics.newBuilder()
              .setCpuUtilization(cpuUtilization)
              .setUsedHeapMemory(heapCurrent);
      // MemoryInfo may return this as -1 if it cannot be computed
      if (heapMax != -1) {
        builder.setMaxHeapMemory(heapMax);
      }
      CoordinatorMetricsProto.GetMetricsResponse response =
          CoordinatorMetricsProto.GetMetricsResponse.newBuilder()
              .setCoordinatorMetrics(builder)
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
    }
  }

  private double calculateCpuUtilization(ThreadsIterator threads) {
    if (!threads.hasNext()) {
      return 0;
    }

    ThreadsIterator.ThreadSummary summary = (ThreadsIterator.ThreadSummary) threads.next();
    if (summary.cores == null || summary.cores == 0) {
      throw new IllegalStateException("Expected valid number of cores, got: " + summary.cores);
    }

    // Each thread summary has this, but it is a thread-agnostic value so fetch it only once for
    // clarity
    int numCores = summary.cores;
    double processCpuTime = 0;
    while (true) {
      // (0,100) representing number of centiseconds of cpu time used by thread over the last second
      double threadCpuTime = summary.cpu_time == null ? 0 : summary.cpu_time;
      processCpuTime += threadCpuTime;
      if (!threads.hasNext()) {
        break;
      }
      summary = (ThreadsIterator.ThreadSummary) threads.next();
    }

    // We summed the cpu time (in centiseconds) across all threads and divide by the number of cores
    // to calculate the percentage of time the processor was executing our threads.
    // E.g. If exactly half of all available clock cycles were dedicated to this coordinator
    // process, the calculation might look like one of the following:
    // Thread 1 cpu time = 100 cs, Thread 2 cpu time = 0, 2 cores -> 50% utilization, or
    // Thread 1 cpu time = 75 cs, Thread 2 cpu time = 25 cs, 2 cores -> 50% utilization, or
    // Thread 1 cpu time = 25 cs, Thread 2 cpu time = 25 cs, 1 core -> 50% utilization, etc
    return processCpuTime / numCores;
  }
}
