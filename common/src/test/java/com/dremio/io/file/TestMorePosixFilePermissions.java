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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.test.DremioTest;

/**
 * Test class for {@code MorePosixFilePermissions}
 */
public class TestMorePosixFilePermissions extends DremioTest {

  /**
   * Test class for {@code MorePosixFilePermissions#fromOctalMode(int)}
   */
  @RunWith(Parameterized.class)
  public static class TestFromOctalMode extends DremioTest {
    private final Set<PosixFilePermission> expected;
    private final int mode;

    @Parameterized.Parameters
    public static Iterable<Object[]> getTestCases() {
      // Brute forcing to list all cases
      List<Set<PosixFilePermission>> combinations = new ArrayList<>();
      combinations.add(EnumSet.noneOf(PosixFilePermission.class));
      for (PosixFilePermission permission: PosixFilePermission.values()) {
        // Duplicate the list
        List<Set<PosixFilePermission>> newCombinations = new ArrayList<>(combinations);
        for (Set<PosixFilePermission> newCombination: newCombinations) {
          newCombination.add(permission);
        }
        combinations.addAll(newCombinations);
      }

      List<Object[]> testCases = new ArrayList<>(combinations.size());
      for(Set<PosixFilePermission> combination: combinations) {
        int mode = 0;

        for (PosixFilePermission permission: combination) {
          switch(permission) {
          case OWNER_READ:
            mode += 0400;
            break;
          case OWNER_WRITE:
            mode += 0200;
            break;
          case OWNER_EXECUTE:
            mode += 0100;
            break;
          case GROUP_READ:
            mode += 0040;
            break;
          case GROUP_WRITE:
            mode += 0020;
            break;
          case GROUP_EXECUTE:
            mode += 0010;
            break;
          case OTHERS_READ:
            mode += 0004;
            break;
          case OTHERS_WRITE:
            mode += 0002;
            break;
          case OTHERS_EXECUTE:
            mode += 0001;
            break;
          }
        }

        testCases.add(new Object[] {combination, mode});
      }
      return testCases;
    }
    public TestFromOctalMode(Set<PosixFilePermission> expected, int mode) {
      this.expected = expected;
      this.mode = mode;
    }

    @Test
    public void checkFromOctalModeInt() {
      assertThat(MorePosixFilePermissions.fromOctalMode(mode)).isEqualTo(expected);
    }

    @Test
    public void checkFromOctalModeString() {
      String s = Integer.toOctalString(mode);
      assertThat(MorePosixFilePermissions.fromOctalMode(s)).isEqualTo(expected);
    }
  }

  @Test
  public void testFromOctalModeWithIllegalMode() {
    assertFails(() -> MorePosixFilePermissions.fromOctalMode(-1));
    assertFails(() -> MorePosixFilePermissions.fromOctalMode(512));
    assertFails(() -> MorePosixFilePermissions.fromOctalMode(Integer.MIN_VALUE));
    assertFails(() -> MorePosixFilePermissions.fromOctalMode(Integer.MAX_VALUE));
    assertFails(() -> MorePosixFilePermissions.fromOctalMode("-1"));
    assertFails(() -> MorePosixFilePermissions.fromOctalMode("8"));
    assertFails(() -> MorePosixFilePermissions.fromOctalMode(""));
    assertFails(() -> MorePosixFilePermissions.fromOctalMode("foo"));
  }

  public void assertFails(Runnable r) {
    assertThatThrownBy(r::run)
      .isInstanceOf(IllegalArgumentException.class);
  }
}
