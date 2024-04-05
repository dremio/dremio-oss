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
package com.dremio.datastore.indexed;

/**
 * Wrapper around the index commit operation. To be used in a try-with-resources block, use {@link
 * #open} before the commit operation to get a {@link CommitCloser} that should be {@link
 * CommitCloser#close closed} after the commit operation.
 */
public interface CommitWrapper {

  /**
   * Open the wrapper before committing.
   *
   * @param storeName store name
   * @return commit closer
   */
  CommitCloser open(String storeName);

  /** Commit closer. */
  abstract class CommitCloser implements AutoCloseable {

    private boolean succeeded = false;

    /** Must be invoked on successful commit. */
    public void succeeded() {
      succeeded = true;
    }

    /** Callback on successful close. */
    protected abstract void onClose();

    /** Close the wrapper after committing. */
    @Override
    public void close() {
      if (succeeded) {
        onClose();
      }
    }
  }

  /** Sink implementation. */
  CommitWrapper NO_OP =
      storeName ->
          new CommitCloser() {
            @Override
            public void onClose() {}
          };
}
