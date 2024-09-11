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
package com.dremio.exec.planner.events;

import com.dremio.common.util.Closeable;

/** A basic event bus interface for planning to limit coupling between planning components. */
public interface PlannerEventBus {
  /**
   * Register a handler for a specific type of event.
   *
   * @param plannerEventHandler the handler to register
   * @return a closeable that can be used to unregister the handler
   */
  Closeable register(PlannerEventHandler<? extends PlannerEvent> plannerEventHandler);

  /**
   * Register handlers for specific event.
   *
   * @param plannerEventHandler the handler to register
   * @return a closeable that can be used to unregister the handler.
   */
  Closeable register(PlannerEventHandler<? extends PlannerEvent>... handlers);

  /**
   * Dispatch an event to all handlers that support it.
   *
   * <p>Note this is not a chain of responsibility pattern, all handlers will be called.
   *
   * @param event the event to dispatch
   */
  void dispatch(PlannerEvent event);
}
