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
package com.dremio.sabot.exec.heap;

/** Interface used by operators to add or remove heap memory participants. */
public interface HeapLowMemController {
  /**
   * Adds a heap memory participant whose heap memory overhead can be continuously tracked. Such
   * participants are assumed to be continuously growing its heap proportional to incoming data
   * batches.
   *
   * @param participantId a uniqueId that uniquely identifies a participant
   * @param participantOverhead the heap overhead per batch for this participant. Note that this is
   *     just a number that signifies a comparative overhead (i.e fatness) w.r.t other participants
   *     and need not be an exact heap usage number. For e.g number of column vectors in a batch
   *     could be a good fatness indicator.
   * @return an interface for the participant to update its continuous growth.
   */
  HeapLowMemParticipant addParticipant(String participantId, int participantOverhead);

  /**
   * Removes this participant. Called by the participant when the participant is done with using
   * heap. For a partition within a spilling operator, this typically coincides with either a
   * spilling event or a state machine change event from consumption to production.
   *
   * @param participantId Id that uniquely identifies the participant.
   */
  void removeParticipant(String participantId, int participantOverhead);
}
