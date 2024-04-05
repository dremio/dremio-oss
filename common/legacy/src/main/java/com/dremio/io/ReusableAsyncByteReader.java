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

import com.google.common.base.Preconditions;

/** An async byte reader that can be reused */
public abstract class ReusableAsyncByteReader implements AsyncByteReader {
  private int refCount = 1;

  /**
   * Increments the ref count on the object
   *
   * @return true if the ref could be successfully taken.
   */
  public final boolean tryAddRef() {
    synchronized (this) {
      if (refCount > 0) {
        ++refCount;
        return true;
      }
    }
    return false;
  }

  /**
   * Releases a ref on the object
   *
   * @return true if this was the final ref.
   */
  public final boolean releaseRef() {
    synchronized (this) {
      Preconditions.checkArgument(
          refCount > 0, "Illegal state while dropping ref on async byte reader");
      --refCount;
      return refCount == 0;
    }
  }

  /**
   * Implement this method to close any resources on final close
   *
   * @throws Exception
   */
  protected void onClose() throws Exception {}

  @Override
  public final void close() throws Exception {
    boolean isFinal = releaseRef();
    if (isFinal) {
      onClose();
    }
  }
}
