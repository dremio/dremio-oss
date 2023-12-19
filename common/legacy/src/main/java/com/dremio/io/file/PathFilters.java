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
package com.dremio.io.file;

import java.util.function.Predicate;

import com.google.common.base.Preconditions;

/**
 * Contains commonly used path filter predicates
 */
public final class PathFilters {

  private PathFilters() {
  }

  /**
   * Filter accepting any file
   */
  public static final Predicate<Path> ALL_FILES = unused -> true ;

  /**
   * Filters out file names which are considered hidden.
   *
   * Hidden files starts with either dot (.) or underscore (_) characters. This includes
   * mapreduce files/directories like "_SUCCESS" success file markers, or "_logs" job
   * log output
   */
  public static final Predicate<Path> NO_HIDDEN_FILES = (Path path) -> {
    final String name = path.getName();
    if (name.isEmpty()) {
      // Is that even possible?
      return true;
    }

    switch (name.charAt(0)) {
    // Hidden files
    case '.':
    case '_':
      return false;

    default:
      return true;
    }
  };

  /**
   * Creates a path predicates to filter files based on their prefix
   *
   * The suffix is matched against the filename, not the whole path.
   *
   * @param suffix the filename prefix
   * @return a path predicate instance
   */
  public static Predicate<Path> startsWith(String prefix) {
    Preconditions.checkNotNull(prefix);

    return path -> path.getName().startsWith(prefix);
  }

  /**
   * Creates a path predicates to filter files based on their suffix
   *
   * The suffix is matched against the filename, not the whole path.
   *
   * @param suffix the filename suffix
   * @return a path predicate instance
   */
  public static Predicate<Path> endsWith(String suffix) {
    Preconditions.checkNotNull(suffix);

    return path -> path.getName().endsWith(suffix);
  }
}
