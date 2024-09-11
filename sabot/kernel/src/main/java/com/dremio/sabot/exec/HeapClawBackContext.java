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

public class HeapClawBackContext {
  private final Trigger trigger;
  private final Throwable cause;
  private final String failContext;

  public HeapClawBackContext(Throwable cause, String failContext, Trigger trigger) {
    this.trigger = trigger;
    this.cause = cause;
    this.failContext = failContext;
  }

  public Trigger getTrigger() {
    return trigger;
  }

  public Throwable getCause() {
    return cause;
  }

  public String getFailContext() {
    return failContext;
  }

  public enum Trigger {
    MEMORY_ARBITER,
    HEAP_MONITOR,
    OTHER
  }
}
