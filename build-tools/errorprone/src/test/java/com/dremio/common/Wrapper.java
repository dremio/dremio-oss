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
package com.dremio.common;

import com.dremio.errorprone.ImplementBothUnwrapAndIsWrapperForTest;

/**
 * Dummy class to replace real com.dremio.common.Wrapper without causing cyclic Maven dependencies.
 *
 * <p>Used in {@link ImplementBothUnwrapAndIsWrapperForTest}.
 */
public interface Wrapper {

  default <T> T unwrap(Class<T> clazz) {
    return null;
  }

  default boolean isWrapperFor(Class<?> clazz) {
    return false;
  }
}
