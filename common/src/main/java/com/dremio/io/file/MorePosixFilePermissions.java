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

import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * Supplementary methods for {@code java.nio.file.attribute.PosixFilePermissions}
 */
public final class MorePosixFilePermissions {

  private MorePosixFilePermissions() {
  }

  /*
   * Note that PosixFilePermission values matches bit order (Big Endian)
   */
  private static final PosixFilePermission[] PERMISSIONS = PosixFilePermission.values();
  private static final int PERMISSIONS_LENGTH = PERMISSIONS.length;

  private static final int MAX_MODE = (1 << PERMISSIONS_LENGTH) - 1;

  /**
   * Converts octal mode permission into a set of {@code PosixFilePermission}
   * @param mode permission octal mode as defined by POSIX. Only read/write/execute bits for owner/group/others is supported
   * @return a set of permission
   * @throws IllegalArgumentException if mode is less than 0 or greater than 0777
   */
  public static Set<PosixFilePermission> fromOctalMode(int mode) {
    Preconditions.checkArgument(0 <= mode && mode <= MAX_MODE, "mode should be between 0 and 0777");

    final Set<PosixFilePermission> result = EnumSet.noneOf(PosixFilePermission.class);
    // Start with most significant bit
    int mask = 1 << (PERMISSIONS_LENGTH - 1);
    for (PosixFilePermission permission: PERMISSIONS) {
      if ((mode & mask) != 0) {
        result.add(permission);
      }
      // Shift bit to the right
      mask = mask >> 1;
    }

    return result;
  }

  /**
   * Converts octal mode permission into a set of {@code PosixFilePermission}
   * @param mode permission octal mode as defined by POSIX. Only read/write/execute bits for owner/group/others is supported
   * @return a set of permission
   * @throws IllegalArgumentException if mode is less than 0 or greater than 0777, or if mode does not represent an octal number
   */
  public static Set<PosixFilePermission> fromOctalMode(String mode) {
    int m = Integer.parseInt(mode, 8);
    return fromOctalMode(m);
  }

}
