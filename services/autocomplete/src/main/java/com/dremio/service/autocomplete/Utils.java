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
package com.dremio.service.autocomplete;

import java.util.Optional;

import com.google.common.collect.ImmutableList;

/**
 * Utils class used throughout the Autocomplete Engine.
 */
public final class Utils {
  private Utils() {}

  public static Optional<Integer> indexOf(final ImmutableList<DremioToken> haystack, final int needle) {
    for (int index = 0; index < haystack.size(); index++) {
      if (haystack.get(index).getKind() == needle) {
        return Optional.of(index);
      }
    }

    return Optional.empty();
  }

  public static Optional<Integer> lastIndexOf(final ImmutableList<DremioToken> haystack, final int needle) {
    for (int index = haystack.size() - 1; index >= 0; index--) {
      if (haystack.get(index).getKind() == needle) {
        return Optional.of(index);
      }
    }

    return Optional.empty();
  }

  public static Optional<Integer> min(Optional<Integer> a, Optional<Integer> b) {
    if (a.isPresent() && b.isPresent()) {
      return Optional.of(Math.min(a.get(), b.get()));
    } else if (a.isPresent()) {
      return a;
    } else if (b.isPresent()) {
      return b;
    } else {
      return Optional.empty();
    }
  }

  public static Optional<Integer> max(Optional<Integer> a, Optional<Integer> b) {
    if (a.isPresent() && b.isPresent()) {
      return Optional.of(Math.max(a.get(), b.get()));
    } else if (a.isPresent()) {
      return a;
    } else if (b.isPresent()) {
      return b;
    } else {
      return Optional.empty();
    }
  }
}
