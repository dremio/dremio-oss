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
package com.dremio.sabot.exec.cursors;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/** Impl of FileCursorManager : uses a hash-map key-ed by the uniqueId. */
public class FileCursorManagerFactoryImpl implements FileCursorManagerFactory {
  private final Map<String, FileCursorManager> map = new HashMap<>();
  private boolean allRegistrationsDone;

  @Override
  public synchronized FileCursorManager getManager(String uniqueId) {
    Preconditions.checkState(!allRegistrationsDone);
    return map.computeIfAbsent(uniqueId, FileCursorManagerImpl::new);
  }

  @Override
  public synchronized void notifyAllRegistrationsDone() {
    allRegistrationsDone = true;
    for (Map.Entry<String, FileCursorManager> entry : map.entrySet()) {
      entry.getValue().notifyAllRegistrationsDone();
    }
  }
}
