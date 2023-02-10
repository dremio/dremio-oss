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
package com.dremio.service.reflection;

/**
 * Reflection Status computed from the reflection goal, entry, and existing materializations
 */
public class ReflectionStatus {

  /**
   * Overall status that describes the general state of the reflection
   */
  public enum COMBINED_STATUS {
    NONE,
    CAN_ACCELERATE,
    CAN_ACCELERATE_WITH_FAILURES,
    REFRESHING,
    FAILED,
    EXPIRED,
    DISABLED,
    INVALID,
    INCOMPLETE,
    CANNOT_ACCELERATE_SCHEDULED,
    CANNOT_ACCELERATE_MANUAL
  }

  /**
   * Reflection config (definition) validity status
   *
   */
  public enum CONFIG_STATUS {
    OK,
    INVALID
  }

  /**
   * Reflection refresh status.  This status tells the user what the reflection manager is going to do next
   * with the reflection.  It is not whether the reflection has a "never refresh" policy because even these reflections
   * will pass through SCHEDULED status when they are newly created.
   */
  public enum REFRESH_STATUS {
    MANUAL,     // reflection is setup to refresh manually
    SCHEDULED,  // next reflection refresh will occur according to the refresh policy
    RUNNING,    // reflection refresh currently running
    GIVEN_UP    // reflection is in failed state, no more refresh
  }

  /**
   * Reflection availability status
   */
  public enum AVAILABILITY_STATUS {
    NONE,       // reflection has no materialization at all
    INCOMPLETE, // reflection has no valid materialization, and latest materialization has missing pdfs data nodes
    EXPIRED,    // reflection has no valid materialization, and latest materialization expired
    AVAILABLE   // reflection has a valid materialization
  }

  public enum REFRESH_METHOD {
    NONE,         // new reflection - we don't know method until it is planned
    FULL,         // after planning, it's always a full refresh
    INCREMENTAL   // after planning, we can use incremental refreshes
  }

  private final CONFIG_STATUS configStatus;
  private final REFRESH_STATUS refreshStatus;
  private final AVAILABILITY_STATUS availabilityStatus;
  private final COMBINED_STATUS combinedStatus;
  private final REFRESH_METHOD refreshMethod;
  private final int numFailures;
  private final long lastDataFetch;
  private final long expiresAt;
  private final long lastRefreshDuration;



  public ReflectionStatus(boolean reflectionEnabled, CONFIG_STATUS configStatus, REFRESH_STATUS refreshStatus,
      AVAILABILITY_STATUS availabilityStatus, int numFailures, long lastDataFetch, long expiresAt,
      REFRESH_METHOD refreshMethod, long lastRefreshDuration) {
    this.configStatus = configStatus;
    this.refreshStatus = refreshStatus;
    this.availabilityStatus = availabilityStatus;
    this.refreshMethod = refreshMethod;
    this.combinedStatus = computeCombinedStatus(reflectionEnabled, configStatus, refreshStatus, availabilityStatus, numFailures > 0);
    this.numFailures = numFailures;
    this.lastDataFetch = lastDataFetch;
    this.expiresAt = expiresAt;
    this.lastRefreshDuration = lastRefreshDuration;
  }

  public CONFIG_STATUS getConfigStatus() {
    return configStatus;
  }

  public REFRESH_STATUS getRefreshStatus() {
    return refreshStatus;
  }

  public AVAILABILITY_STATUS getAvailabilityStatus() {
    return availabilityStatus;
  }

  public COMBINED_STATUS getCombinedStatus() {
    return combinedStatus;
  }

  public REFRESH_METHOD getRefreshMethod() {
    return refreshMethod;
  }

  public int getNumFailures() {
    return numFailures;
  }

  public long getLastDataFetch() {
    return lastDataFetch;
  }

  public long getExpiresAt() {
    return expiresAt;
  }

  public long getLastRefreshDuration() { return lastRefreshDuration; }


  /**
   * COMBINED_STATUS is only shown in sys.reflections and not directly in the UI.
   *
   * @param reflectionEnabled
   * @param configStatus
   * @param refreshStatus
   * @param availabilityStatus
   * @param hasFailures
   * @return
   */
  private static COMBINED_STATUS computeCombinedStatus(boolean reflectionEnabled, final CONFIG_STATUS configStatus,
      REFRESH_STATUS refreshStatus, AVAILABILITY_STATUS availabilityStatus, boolean hasFailures) {
    if (!reflectionEnabled) {
      return COMBINED_STATUS.DISABLED;
    }

    if (configStatus == CONFIG_STATUS.INVALID) {
      return COMBINED_STATUS.INVALID;
    }

    if (refreshStatus == REFRESH_STATUS.GIVEN_UP) {
      return COMBINED_STATUS.FAILED;
    } else if (availabilityStatus == AVAILABILITY_STATUS.INCOMPLETE) {
      return COMBINED_STATUS.INCOMPLETE;
    } else if (availabilityStatus == AVAILABILITY_STATUS.EXPIRED) {
      return COMBINED_STATUS.EXPIRED;
    } else if (refreshStatus == REFRESH_STATUS.RUNNING) {
      if (availabilityStatus == AVAILABILITY_STATUS.AVAILABLE) {
        return COMBINED_STATUS.CAN_ACCELERATE;
      } else {
        return COMBINED_STATUS.REFRESHING;
      }
    } else if (availabilityStatus == AVAILABILITY_STATUS.AVAILABLE) {
      if (hasFailures) {
        return COMBINED_STATUS.CAN_ACCELERATE_WITH_FAILURES;
      } else {
        return COMBINED_STATUS.CAN_ACCELERATE;
      }
    } else if (refreshStatus == REFRESH_STATUS.SCHEDULED) {
      return COMBINED_STATUS.CANNOT_ACCELERATE_SCHEDULED;
    } else if (refreshStatus == REFRESH_STATUS.MANUAL) {
      return COMBINED_STATUS.CANNOT_ACCELERATE_MANUAL;
    }

    return COMBINED_STATUS.NONE; // we should never reach this
  }
}
