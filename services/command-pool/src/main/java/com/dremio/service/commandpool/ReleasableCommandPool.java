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
package com.dremio.service.commandpool;

import com.dremio.common.util.Closeable;

/**
 * Interface for a releaseable command pool. In addition to the CommandPool, this class provides a
 * method for the caller to release the command pool slot that it is currently using
 */
public interface ReleasableCommandPool extends CommandPool {
  /**
   * This API is a helper API provided to callers to know if they are holding a command pool slot
   *
   * @return
   */
  boolean amHoldingSlot();

  /**
   * Releases the command pool slot and returns a Closeable that re-acquires the command pool slot.
   * This is expected to be used in a try-with-resources block
   *
   * @return
   */
  Closeable releaseAndReacquireSlot();
}
