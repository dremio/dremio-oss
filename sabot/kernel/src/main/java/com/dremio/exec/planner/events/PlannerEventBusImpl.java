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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A naive in thread event bus for planner events. If {@link PlannerEventBus} replaces {@link
 * com.dremio.exec.planner.observer.AttemptObserver}, then a threaded one will likely be needed.
 */
public class PlannerEventBusImpl implements PlannerEventBus {
  private final Map<
          Class<? extends PlannerEvent>, List<PlannerEventHandler<? extends PlannerEvent>>>
      eventTypeToHandlers = new HashMap<>();

  @Override
  public Closeable register(PlannerEventHandler<? extends PlannerEvent> plannerEventHandler) {
    if (PlannerEventHandler.NO_OP == plannerEventHandler) {
      return () -> {};
    }
    List<PlannerEventHandler<? extends PlannerEvent>> plannerEventHandlerList =
        eventTypeToHandlers.computeIfAbsent(plannerEventHandler.supports(), k -> new ArrayList<>());
    plannerEventHandlerList.add(plannerEventHandler);
    return () -> plannerEventHandlerList.remove(plannerEventHandler);
  }

  @Override
  public void dispatch(PlannerEvent event) {

    for (PlannerEventHandler<? extends PlannerEvent> plannerEventHandler :
        eventTypeToHandlers.getOrDefault(event.getClass(), List.of())) {
      doHandle(plannerEventHandler, event);
    }
  }

  private static void doHandle(PlannerEventHandler eventHandler, PlannerEvent event) {
    eventHandler.handle(event);
  }
}
