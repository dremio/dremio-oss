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
package com.dremio.io;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;

/**
 * An async byte reader that can be reused
 */
public abstract class ReusableAsyncByteReader implements AsyncByteReader {
  private final AtomicLong refCount = new AtomicLong(1);

  /**
   * Increments the ref count on the object
   * @return the incremented ref count
   */
  public final long addRef() {
    Preconditions.checkArgument(refCount.get() > 0, "Illegal state while reusing async byte reader");
    return refCount.incrementAndGet();
  }

  /**
   * Releases a ref on the object
   * @return the decremented ref count
   */
  public final long releaseRef() {
    Preconditions.checkArgument(refCount.get() > 0, "Illegal state while dropping ref on async byte reader");
    return refCount.decrementAndGet();
  }

  /**
   * Implement this method to close any resources on final close
   * @throws Exception
   */
  protected void onClose() throws Exception {
  }

  @Override
  public final void close() throws Exception {
    long currentRef = releaseRef();
    Preconditions.checkArgument(currentRef >= 0, "Invalid ref count on async byte reader");
    if (currentRef == 0) {
      onClose();
    }
  }
}
