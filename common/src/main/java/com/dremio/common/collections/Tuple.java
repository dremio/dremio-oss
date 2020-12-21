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
package com.dremio.common.collections;

import java.util.Objects;

/**
 * An immutable ordered sequence of two elements
 *
 * @param <F> first element
 * @param <S> second element
 */
public class Tuple<F, S> {
  public final F first;
  public final S second;

  protected Tuple(final F first, final S second) {
    this.first = first;
    this.second = second;
  }

  public static <F, S> Tuple<F, S> of(final F first, final S second) {
    return new Tuple<>(first, second);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tuple<?, ?> tuple = (Tuple<?, ?>) o;
    return Objects.equals(first, tuple.first) && Objects.equals(second, tuple.second);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  @Override
  public String toString() {
    return "Tuple{first=" + first + ", second=" + second + "}";
  }
}
