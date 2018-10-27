/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
   * Invoked when the caller is unable to build a query ticket
   * The call is supposed to handle the exception and not throw anything. This function might be invoked in the
   * context of another thread, where throwing an exception is not an option.
   */
  void unableToBuildQuery(Exception e);
}
