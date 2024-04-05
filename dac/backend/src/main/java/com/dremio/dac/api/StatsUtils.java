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
package com.dremio.dac.api;

import com.dremio.service.Pointer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Timestamp;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Misc set of utils functions. */
public class StatsUtils {
  private static final long nMilliSecInADay = 1000 * 24 * 60 * 60L;
  // since user stats and job stats are typically called for 7 days - setting a min of 14 threads
  private static final int STATS_THREAD_POOL_SIZE =
      Math.max(14, Runtime.getRuntime().availableProcessors() - 1);

  private static final ExecutorService statsExecutorService =
      Executors.newFixedThreadPool(
          STATS_THREAD_POOL_SIZE,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("stats-pool-%d").build());

  /**
   * Create a list of pairs - one pair for each date between startEpoch and endEpoch. Each pair
   * contains start and end epoch times.
   *
   * <p>Invariants Start epoch of each pair >= startEpoch End epoch of each pair <= endEpoch start
   * epoch of each pair <= end epoch of each pair
   *
   * <p>For eg: startEpoch 1634748029001 i.e Wednesday, 20 October 2021 16:40:29.000001 endEpoch
   * 1634920829002 i.e Friday, 22 October 2021 16:40:29.000002
   *
   * <p>Result list will have epoch equivalent of results = { [Wednesday, 20 October 2021
   * 16:40:29.000001, Wednesday, 20 October 2021 23:59:59.999999], [Thursday, 21 October 2021
   * 00:00:00.000000, Thursday, 21 October 2021 23:59:59.999999] [Friday, 22 October 2021
   * 00:00:00.000000, Friday, 22 October 2021 16:40:29.000002] }
   *
   * @param startEpoch
   * @param endEpoch
   * @return
   * @throws ParseException
   */
  public static List<Long[]> createDateWiseParameterBatches(long startEpoch, long endEpoch)
      throws ParseException {
    List<Long[]> batches = new ArrayList<>();
    long iterStartDate = startEpoch;
    // compute iterEndDate as last second of same day as iterStartDate
    long iterEndDate =
        Instant.ofEpochMilli(iterStartDate)
            .atZone(ZoneId.systemDefault())
            .toLocalDate()
            .atTime(LocalTime.MAX)
            .atZone(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli();

    iterEndDate = Math.min(iterEndDate, endEpoch);

    do {
      Long[] pair = new Long[2];
      pair[0] = iterStartDate;
      pair[1] = iterEndDate;

      batches.add(pair);

      iterStartDate = iterEndDate + 1;
      iterEndDate = iterStartDate + nMilliSecInADay - 1;
      iterEndDate = Math.min(iterEndDate, endEpoch);
    } while (iterStartDate < endEpoch);

    return batches;
  }

  public static Timestamp convert(long epochInMilliSeconds) {
    return Timestamp.newBuilder()
        .setSeconds(epochInMilliSeconds / 1000)
        .setNanos((int) (epochInMilliSeconds % 1000) * 1_000_000)
        .build();
  }

  public static void executeDateWise(long startEpoch, long endEpoch, Consumer<Long[]> consumer)
      throws Exception {
    Pointer<Exception> exceptionPointer = new Pointer<>();
    List<Long[]> batches = createDateWiseParameterBatches(startEpoch, endEpoch);
    CountDownLatch latch = new CountDownLatch(batches.size());
    batches.stream()
        .forEach(
            pair ->
                statsExecutorService.execute(
                    () -> {
                      try {
                        consumer.accept(pair);
                      } catch (Exception err) {
                        exceptionPointer.value = err;
                        // exit on first exception.
                        while (latch.getCount() > 0) {
                          latch.countDown();
                        }
                      }
                      latch.countDown();
                    }));

    latch.await(100, TimeUnit.SECONDS);

    if (exceptionPointer.value != null) {
      throw exceptionPointer.value;
    }
  }
}
