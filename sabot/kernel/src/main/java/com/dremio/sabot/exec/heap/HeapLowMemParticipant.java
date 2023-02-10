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

/**
 * Interface that is invoked by low memory checker participants who are interested in informing their memory
 * usage overhead and is also interested in knowing whether they have been chosen as a victim to shed memory
 * and take action.
 * <p>
 * For instance, a memory partition inside a spilling operator could be a participant which continuously adds
 * overhead (whenever a new incoming batch is processed) and also checks if this participant has been chosen as a
 * victim to shed its heap memory.
 * </p>
 */
public interface HeapLowMemParticipant {
  /**
   * Tracks added batches. This is an indication that memory usage of this participant is increasing. This is
   * typical of all spilling operators as they have to keep metadata in heap memory as the query progresses.
   *
   * @param numBatches number of batches, typically one, but could be more depending on implementation.
   */
  void addBatches(int numBatches);

  /**
   * Returns true, if this participant has been chosen as a victim.
   *
   * <p>
   * If this participant has been chosen as a victim, the expected behaviour from the victim is to shed all
   * its heap memory usage (e.g a memory partition in a spilling operator can switch to a disk partition) and
   * remove itself as a participant.
   * </p>
   * @return true if this participant is the chosen victim, false otherwise
   */
  boolean isVictim();
}
