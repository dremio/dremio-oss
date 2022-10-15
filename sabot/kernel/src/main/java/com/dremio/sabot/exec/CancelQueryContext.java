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
package com.dremio.sabot.exec;

/**
 * Placeholder to hold cancel related context used for killing queries by
 * heap monitor.
 */
public class CancelQueryContext {
  private final String cancelReason;
  private final String cancelContext;
  private final boolean isCancelledByHeapMonitor;

  public CancelQueryContext(String cancelReason, String cancelContext,
                            boolean isCancelledByHeapMonitor) {
    this.cancelReason = cancelReason;
    this.cancelContext = cancelContext;
    this.isCancelledByHeapMonitor = isCancelledByHeapMonitor;
  }

  public String getCancelReason() {
    return cancelReason;
  }

  public String getCancelContext() {
    return cancelContext;
  }

  public boolean isCancelledByHeapMonitor() {
    return isCancelledByHeapMonitor;
  }
}
