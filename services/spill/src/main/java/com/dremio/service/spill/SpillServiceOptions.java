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
package com.dremio.service.spill;

/** Options used by the SpillService */
public interface SpillServiceOptions {
  /**
   * Should the health check be enabled. Please note: setting this parameter is a necessary but not
   * sufficient condition for enabling the health check. In addition to this parameter being set,
   * the spill directory needs to be on a local filesystem
   */
  boolean enableHealthCheck();

  /**
   * The minimum disk space, in bytes, below which a spill directory will be marked unhealthy.
   * Please note, only the higher value (in bytes) between this and {@link
   * #minDiskSpacePercentage()} will be used
   */
  long minDiskSpace();

  /**
   * The minimum disk space, in percent, below which a spill directory will be marked unhealthy.
   * Please note, only the higher value (in bytes) between this and {@link #minDiskSpace()} will be
   * used
   */
  double minDiskSpacePercentage();

  /** Interval, in milliseconds, at which a spill directory will be checked for health */
  long healthCheckInterval();

  /**
   * Interval, in milliseconds, at which the spill service health check should check for spill
   * sub-directories older than the {@link #spillSweepThreshold()} Please note: the health check
   * will still operate at the granularity of the {@link #healthCheckInterval()}
   */
  long spillSweepInterval();

  /**
   * At each {@link #spillSweepInterval()}, any sub-directories older than this threshold (defined
   * in milliseconds) will be erased. This is useful for limiting the detritus remaining from
   * unresponsive spill drives.
   */
  long spillSweepThreshold();
}
