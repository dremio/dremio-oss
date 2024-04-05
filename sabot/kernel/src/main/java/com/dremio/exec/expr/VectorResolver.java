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
package com.dremio.exec.expr;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.google.common.base.Preconditions;
import org.apache.arrow.vector.ValueVector;

public class VectorResolver {
  public static <T extends ValueVector> T[] hyper(
      VectorAccessible incoming, Class<T> vectorClass, int... ids) {
    VectorWrapper<T> wrapper = incoming.getValueAccessorById(vectorClass, ids);
    Preconditions.checkNotNull(wrapper, "Failure while loading vector. Vector not found.");
    Preconditions.checkArgument(
        wrapper.isHyper(),
        "Failure while loading vector. Vector was expected to be hyper but turned out to be simple.");
    return wrapper.getValueVectors();
  }

  public static <T extends ValueVector> T simple(
      VectorAccessible incoming, Class<T> vectorClass, int... ids) {
    VectorWrapper<T> wrapper = incoming.getValueAccessorById(vectorClass, ids);
    Preconditions.checkNotNull(wrapper, "Failure while loading vector. Vector not found.");
    Preconditions.checkArgument(
        !wrapper.isHyper(),
        "Failure while loading vector. Vector was expected to be simple but turned out to be hyper.");
    return wrapper.getValueVector();
  }
}
