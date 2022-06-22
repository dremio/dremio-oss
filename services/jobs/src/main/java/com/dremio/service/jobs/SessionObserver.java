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
package com.dremio.service.jobs;

import com.dremio.sabot.rpc.user.UserSession;

/**
 * Observer for handling session based properties.
 */
interface SessionObserver {
  SessionObserver NO_OP = new SessionObserver() {};

  /**
   * Called when the job is completed
   */
  default void onCompleted() {}

  /**
   * Called when the job has failed
   *
   * @param e the exception thrown by the job
   */
  default void onError(Exception e) {}

  default void onSessionModified(UserSession session) {}
}
