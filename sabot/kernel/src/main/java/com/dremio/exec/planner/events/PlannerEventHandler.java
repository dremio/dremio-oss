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

/** A Callback for handling planner events. */
public interface PlannerEventHandler<EVENT extends PlannerEvent> {
  PlannerEventHandler<PlannerEvent> NO_OP =
      new PlannerEventHandler<>() {
        @Override
        public void handle(PlannerEvent event) {}

        @Override
        public Class<PlannerEvent> supports() {
          return PlannerEvent.class;
        }
      };

  /**
   * Handle the event.
   *
   * @param event the event to handle
   */
  void handle(EVENT event);

  /**
   * @return the class of the event that this handler supports. Inheritance is not supported.
   */
  Class<EVENT> supports();
}
