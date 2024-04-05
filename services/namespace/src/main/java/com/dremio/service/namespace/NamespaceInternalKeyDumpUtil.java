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
package com.dremio.service.namespace;

import com.dremio.common.utils.PathUtils;
import java.util.Arrays;

/**
 * Utility class for methods used in debugging and tests.
 *
 * <p>These methods should only be used in debugging and testing, not in production.
 */
final class NamespaceInternalKeyDumpUtil {
  private NamespaceInternalKeyDumpUtil() {}

  /**
   * Converts a key in bytes to a NamespaceInternalKey Object.
   *
   * @param keyBytes key in bytes.
   * @return a NamespaceInternalKey.
   */
  static NamespaceInternalKey parseKey(byte[] keyBytes) {
    String path = extractKey(keyBytes, false);
    return new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)));
  }

  /**
   * Converts provided prefix bytes to an integer.
   *
   * @param prefix prefix in bytes.
   * @return an integer representation of the provided bytes.
   */
  static int prefixBytesToInt(byte[] prefix) {
    int number = 0;
    for (int i = 0; i < NamespaceInternalKey.PREFIX_BYTES_SIZE; ++i) {
      number <<= 8;
      number ^= (prefix[i] & 0xFF);
    }
    return number;
  }

  /**
   * Extracts a string representation of a NamespaceInternalKey from the provided keyBytes.
   *
   * @param keyBytes key byte[] to process.
   * @param includePrefixLevels if true, encodes prefix level in the String.
   * @return String representation of key bytes path encoded with level prefixes if requested.
   */
  static String extractKey(byte[] keyBytes, boolean includePrefixLevels) {
    final StringBuilder builder = new StringBuilder();

    int offset = indexOfDelimiterPrefixStart(keyBytes, 0);
    if (offset != 0) {
      throw new IllegalArgumentException("Key should start with delimiter-prefix-delimiter");
    }
    if (includePrefixLevels) {
      builder.append(extractLevelFromDelimiterPrefix(keyBytes, offset));
      builder.append(NamespaceInternalKey.PATH_DELIMITER);
    }

    offset = offset + NamespaceInternalKey.DELIMITER_PREFIX_DELIMITER_BYTES_SIZE;
    while (true) {
      int index1 = indexOfDelimiterPrefixStart(keyBytes, offset);
      if (index1 == -1) {
        // Copy the remaining array as the last path component.
        builder.append(new String(keyBytes, offset, keyBytes.length - offset));
        break;
      } else {
        builder.append(new String(keyBytes, offset, index1 - offset));
        builder.append(NamespaceInternalKey.PATH_DELIMITER);

        if (includePrefixLevels) {
          builder.append(extractLevelFromDelimiterPrefix(keyBytes, index1));
          builder.append(NamespaceInternalKey.PATH_DELIMITER);
        }

        offset = index1 + NamespaceInternalKey.DELIMITER_PREFIX_DELIMITER_BYTES_SIZE;
      }
    }
    return builder.toString();
  }

  /**
   * Extracts a string representation of a range key from the provided keyBytes.
   *
   * @param keyBytes key byte[] to process.
   * @return String representation of key bytes path encoded with level prefixes.
   */
  static String extractRangeKey(byte[] keyBytes) {
    final StringBuilder builder = new StringBuilder();

    int offset = indexOfDelimiterPrefixStart(keyBytes, 0);
    if (offset != 0) {
      throw new IllegalArgumentException("Key should start with delimiter-prefix-delimiter");
    }
    builder.append(extractLevelFromDelimiterPrefix(keyBytes, 0));
    builder.append(NamespaceInternalKey.PATH_DELIMITER);

    offset = offset + NamespaceInternalKey.DELIMITER_PREFIX_DELIMITER_BYTES_SIZE;
    while (true) {
      int index1 = indexOfDelimiterPrefixStart(keyBytes, offset);
      if (index1 == -1) {
        break;
      }

      builder.append(new String(keyBytes, offset, index1 - offset));
      builder.append(NamespaceInternalKey.PATH_DELIMITER);

      builder.append(extractLevelFromDelimiterPrefix(keyBytes, index1));
      builder.append(NamespaceInternalKey.PATH_DELIMITER);

      offset = index1 + NamespaceInternalKey.DELIMITER_PREFIX_DELIMITER_BYTES_SIZE;
    }
    return builder.toString();
  }

  private static int indexOfDelimiterPrefixStart(byte[] array, int offset) {
    outer:
    for (int i = offset;
        i < array.length - NamespaceInternalKey.DELIMITER_PREFIX_DELIMITER_BYTES_SIZE + 1;
        i++) {
      for (int j = 0; j < NamespaceInternalKey.DELIMITER_BYTES.length; j++) {
        if (array[i + j] != NamespaceInternalKey.DELIMITER_BYTES[j]) {
          continue outer;
        }
      }
      // found the delimiter, now skip past the prefix number and then look for delimiter again
      final int newI =
          i + NamespaceInternalKey.DELIMITER_BYTES.length + NamespaceInternalKey.PREFIX_BYTES_SIZE;
      for (int j = 0; j < NamespaceInternalKey.DELIMITER_BYTES.length; j++) {
        if (array[newI + j] != NamespaceInternalKey.DELIMITER_BYTES[j]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }

  private static int extractLevelFromDelimiterPrefix(byte[] keyBytes, int delimiterStart) {
    return prefixBytesToInt(
        Arrays.copyOfRange(
            keyBytes,
            delimiterStart + NamespaceInternalKey.DELIMITER_BYTES.length,
            delimiterStart
                + NamespaceInternalKey.DELIMITER_BYTES.length
                + NamespaceInternalKey.PREFIX_BYTES_SIZE));
  }
}
