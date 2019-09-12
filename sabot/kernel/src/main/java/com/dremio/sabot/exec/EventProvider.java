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

import java.util.Optional;

import com.dremio.exec.proto.ExecProtos;

/**
 * Provides {@link com.dremio.sabot.exec.fragment.FragmentExecutor} with cancel and early termination messages
 */
public interface EventProvider {

  /**
   * retrieves one of the finished receivers and removes it from the internal container
   *
   * @return finished receiver, null if no early termination message was received since last access to this method
   */
  ExecProtos.FragmentHandle pollFinishedReceiver();

  /**
   * @return reason for failure, if there is one.
   */
  Optional<Throwable> getFailedReason();
}
