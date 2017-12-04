/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.work.protector;

/**
 * Provides a contract between Foreman and QueryManager to indicate
 * Foreman of events about query fragments.
 */
public interface FragmentsStateListener {
  /**
   * QueryManager invokes this callback that all query fragments
   * are in terminal state and the Foreman can take appropriate
   * action in order to reattempt the query for OOM failures.
   * The actual implementation of this method is in
   * Foreman.Observer.
   */
  void allFragmentsRetired();
}
