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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.function.Predicate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.dremio.test.DremioTest;

/**
 * Tests for helpers inside {@code PathFilters}
 */
public class TestPathFilters extends DremioTest {

  /**
   * Tests for {@code PathFilters#NO_HIDDEN_FILES}
   */
  @RunWith(Parameterized.class)
  public static final class TestNoHiddenFiles extends DremioTest {
    private final boolean expected;
    private final Path path;

    @Parameters(name = "{index}: path={1}")
    public static Iterable<Object[]> getTestCases() {
      return Arrays.asList(
          new Object[] { true, "foo" },
          new Object[] { true, "bar" },
          new Object[] { true, "foo.bar" },
          new Object[] { true, "foo_bar" },
          new Object[] { true, "bar." },
          new Object[] { true, "bar_" },
          new Object[] { false, ".foo" },
          new Object[] { false, "_foo" },
          new Object[] { false, "_" },
          new Object[] { false, "_logs" },
          new Object[] { false, "_SUCCESS" },
          new Object[] { true, "/_foo/bar" },
          new Object[] { false, "/_foo/_bar" }
          );
    }

    public TestNoHiddenFiles(boolean expected, String path) {
      this.expected = expected;
      this.path = Path.of(path);
    }

    @Test
    public void test() {
      assertThat(PathFilters.NO_HIDDEN_FILES.test(path), is(expected));
    }
  }


  /**
   * Tests for {@code PathFilters#startsWith(String)}
   */
  @RunWith(Parameterized.class)
  public static final class TestStartsWith extends DremioTest {
    private final boolean expected;
    private final Path path;
    private final Predicate<Path> predicate;

    @Parameters(name = "{index}: path={1}, pattern={2}")
    public static Iterable<Object[]> getTestCases() {
      return Arrays.asList(
          new Object[] { true, "foo" , "" },
          new Object[] { true, "foo" , "f" },
          new Object[] { true, "foo" , "fo" },
          new Object[] { false, "foo" , "oo" },
          new Object[] { true, "bar" , "bar" },
          new Object[] { false, "bar" , "barb" },
          new Object[] { true, "/foo/bar",  "bar"},
          new Object[] { false, "/foo/bar",  "foo"}
          );
    }

    public TestStartsWith(boolean expected, String path, String pattern) {
      this.expected = expected;
      this.path = Path.of(path);
      this.predicate = PathFilters.startsWith(pattern);
    }

    @Test
    public void test() {
      assertThat(predicate.test(path), is(expected));
    }
  }

  /**
   * Tests for {@code PathFilters#startsWith(String)}
   */
  @RunWith(Parameterized.class)
  public static final class TestEndsWith extends DremioTest {
    private final boolean expected;
    private final Path path;
    private final Predicate<Path> predicate;

    @Parameters(name = "{index}: path={1}, pattern={2}")
    public static Iterable<Object[]> getTestCases() {
      return Arrays.asList(
          new Object[] { true, "foo" , "" },
          new Object[] { true, "foo" , "o" },
          new Object[] { true, "foo" , "oo" },
          new Object[] { false, "foo" , "f" },
          new Object[] { true, "bar" , "bar" },
          new Object[] { false, "bar" , "barb" },
          new Object[] { true, "/foo/bar",  "r"},
          new Object[] { false, "/foo/bar",  "o"}
          );
    }

    public TestEndsWith(boolean expected, String path, String pattern) {
      this.expected = expected;
      this.path = Path.of(path);
      this.predicate = PathFilters.endsWith(pattern);
    }

    @Test
    public void test() {
      assertThat(predicate.test(path), is(expected));
    }
  }

}
