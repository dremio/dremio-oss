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
package com.dremio.service.scheduler;

import java.util.Iterator;

import org.threeten.bp.Instant;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.ZoneId;
import org.threeten.bp.temporal.TemporalAdjuster;
import org.threeten.bp.temporal.TemporalAmount;

class BaseSchedule implements Schedule {
  private final TemporalAmount amount;
  private final TemporalAdjuster adjuster;
  private final Instant at;
  private final ZoneId zoneId;

  BaseSchedule(Instant at, TemporalAmount period, TemporalAdjuster adjuster, ZoneId zoneId) {
    this.amount = period;
    this.adjuster = adjuster;
    this.at = at;
    this.zoneId = zoneId;
  }

  @Override
  public TemporalAmount getPeriod() {
    return amount;
  }

  @Override
  public Iterator<Instant> iterator() {
    LocalDateTime at = LocalDateTime.ofInstant(this.at, zoneId);
    LocalDateTime adjustedAt = at.with(adjuster);
    final LocalDateTime start = adjustedAt.isBefore(at)
        ? at.plus(amount).with(adjuster)
        : adjustedAt;

    return new Iterator<Instant>() {
      private LocalDateTime next = start;

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public Instant next() {
        Instant result = next.atZone(zoneId).toInstant();
        next = next.plus(amount).with(adjuster);

        return result;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Schedule iterator does not support remove operation.");
      }
    };
  }
}
