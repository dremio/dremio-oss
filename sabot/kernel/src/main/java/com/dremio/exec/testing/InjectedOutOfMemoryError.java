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
package com.dremio.exec.testing;

/**
 * Special implementation of {@link OutOfMemoryError} that always passes "Direct buffer memory" as
 * error message.
 */
@SuppressWarnings("unused")
public class InjectedOutOfMemoryError extends OutOfMemoryError {

  // this is needed by the exception injection framework
  public InjectedOutOfMemoryError(String str) {
    this();
  }

  public InjectedOutOfMemoryError() {
    super("Direct buffer memory");
  }
}
