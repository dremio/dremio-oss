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
 * Given a query ticket, build the fragments needed for the query, and enqueues the execution of the query
 */
public interface QueryStarter {
  /**
   * Given a ticket, build the fragments needed for the query, and enqueue the execution of the query
   * The query ticket has a child count upped by the caller. The implementation needs to call
   * {@link QueryTicket#release()} on the incoming ticket, or else the ticket will be leaked
   */
  void buildAndStartQuery(QueryTicket ticket);

  /**
   * Gets the size of the query in terms of the number of tasks that will be created.
   * <p>
   * Note: this can be an approximation, if it is more performant to do so. Due to this clients of this
   * API should treat this as an approximation.
   * </p>
   * @return approximate size of query
   */
  int getApproximateQuerySize();

  /**
   * This method is used to determine if the query should use weight based scheduling
   * @return
   */
  default boolean useWeightBasedScheduling() {
    return false;
  }

  /**
   * Invoked when the caller is unable to build a query ticket
   * The call is supposed to handle the exception and not throw anything. This function might be invoked in the
   * context of another thread, where throwing an exception is not an option.
   */
  void unableToBuildQuery(Exception e);
}
