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
package com.dremio.exec.catalog;

/** Metadata sync status. */
class SyncStatus {

  private final boolean fullRefresh;

  private long shallowAdded;
  private long shallowDeleted;
  private long shallowUnchanged;

  private long extendedChanged;
  private long extendedUnchanged;
  private long extendedUnreadable;
  private long extendedDeleted;

  private boolean refreshed;
  private boolean interrupted;

  SyncStatus(boolean fullRefresh) {
    this.fullRefresh = fullRefresh;
  }

  void incrementShallowAdded() {
    shallowAdded++;
  }

  void incrementShallowDeleted() {
    shallowDeleted++;
  }

  void incrementShallowUnchanged() {
    shallowUnchanged++;
  }

  void incrementExtendedChanged() {
    extendedChanged++;
  }

  void incrementExtendedUnchanged() {
    extendedUnchanged++;
  }

  void incrementExtendedUnreadable() {
    extendedUnreadable++;
  }

  void incrementExtendedDeleted() {
    extendedDeleted++;
  }

  void setRefreshed() {
    this.refreshed = true;
  }

  void setInterrupted(boolean interrupted) {
    this.interrupted = interrupted;
  }

  boolean isRefreshed() {
    return refreshed;
  }

  boolean isInterrupted() {
    return interrupted;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(
        String.format(
            "Shallow probed %d datasets: %d added, %d unchanged, %d deleted\n",
            shallowAdded + shallowUnchanged + shallowDeleted,
            shallowAdded,
            shallowUnchanged,
            shallowDeleted));
    if (fullRefresh) {
      builder.append(
          String.format(
              "Deep probed %d queried datasets: %d changed, %d unchanged, %d deleted, %d unreadable\n",
              extendedChanged + extendedUnchanged + extendedDeleted + extendedUnreadable,
              extendedChanged,
              extendedUnchanged,
              extendedDeleted,
              extendedUnreadable));
    }
    return builder.toString();
  }
}
