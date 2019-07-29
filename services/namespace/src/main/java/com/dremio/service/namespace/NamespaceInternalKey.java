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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.SqlUtils;

/**
 * Namespace keys used to model space, folders, sources, datasets names into kvstore key.
 *
 * Namespace keys are designed so that we can list folders or sub folders.
 * In order to use range query on kvstore we encode depth inside namespace key.
 * /a/b/c is   ``2``a``1``b``0``c
 * /a/b is ``1``a``0``b
 * a is ``0``a
 *
 * Range search or listing under a key
 * listing under / we will search for ``0``*
 * listing under /a we will search for ``1``a``0``*
 * listing under /a/b we will search for ``2``a``1``b``0``*
 * listing under /a/b/c we will search for ``3``a``2``b``1``c``0``*
 *
 * Source entities(files, folders, physical datasets) are special since listing is always recursive and
 * there are no guarantees that all parents are in namespace.
 * Depth value for all source entities is set to zero.
 * Namespace doesn't check if parent exists for a GET operation on physical dataset like virtual dataset.
 *
 * Note: this is a package protected class. Never make it a public class.
 **/
class NamespaceInternalKey {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NamespaceInternalKey.class);

  /**
   * Hard coded flag that allows disabling namespace key normalization.
   * The purpose is to have one specific build with this set to false
   */
  private static final boolean ENABLE_KEY_NORMALIZATION = true;

  private static final char PATH_DELIMITER = '.'; // dot separated path
  private static final char QUOTE = SqlUtils.QUOTE; // backticks allowed to escape dot/keywords.
                                                    // TODO: make it independent of SQL
  private static final byte MIN_VALUE = (byte) 0x0;
  private static final byte MAX_VALUE = (byte) 0xFF;

  private static final int PREFIX_BYTES_SIZE = 2;
  /** byte array of size 1 containing 0xFF */
  private static final byte[] TERMINATOR = new byte[] { MAX_VALUE };
  /**
   * "``" in UTF-8.
   * In namespace key, path components are separated by `` which will never be part of the key.
   */
  private static final byte[] DELIMITER_BYTES = format("%c%c", QUOTE, QUOTE).getBytes(UTF_8);

  private static final int DELIMITER_PREFIX_DELIMITER_SIZE =
      DELIMITER_BYTES.length + PREFIX_BYTES_SIZE + DELIMITER_BYTES.length;

  /** prefixes[i] == DELIMITER_BYTES + {i on 2 bytes} + DELIMITER_BYTES */
  private static final byte [][] prefixes = generatePrefixes();
  private static final String NAMESPACE_PATH_FORMAT =
    "Namespace path should be of format <space>.<folder1>.<folder2>....<folderN>.<dataset/file>";

  private static final byte[] ROOT_LOOKUP_START = rootLookupStartKey();
  private static final byte[] ROOT_LOOKUP_END = rootLookupEndKey();

  private static final byte[][] generatePrefixes() {
    // will generate all possible numbers on 2 bytes
    final byte[][] prefixes = new byte[1 << (PREFIX_BYTES_SIZE * 8)][];
    for (int i = 0; i < prefixes.length; ++i) {
      // `` + {i on 2 bytes} + ``
      prefixes[i] = new byte[DELIMITER_PREFIX_DELIMITER_SIZE];
      System.arraycopy(DELIMITER_BYTES, 0, prefixes[i], 0, DELIMITER_BYTES.length);
      System.arraycopy(toPrefixBytes(i), 0, prefixes[i], DELIMITER_BYTES.length, PREFIX_BYTES_SIZE);
      System.arraycopy(DELIMITER_BYTES, 0, prefixes[i], DELIMITER_BYTES.length + PREFIX_BYTES_SIZE, DELIMITER_BYTES.length);
    }
    return prefixes;
  }

  private byte [] keyBytes = null; // lookup key
  private byte[] cachedKey = null; // copy of lookup key keyBytes
  // list folder/lookup keys
  private byte[] cachedRangeStartKey = null, cachedRangeEndKey = null;

  /** length of the typed key in keyBytes*/
  private int keyLength;

  /**
   * dot delimited path
   * with components quoted with back ticks if they are keywords
   */
  private final String namespaceFullPath;
  /** path for this name */
  private final NamespaceKey namespaceKey;

  /**
   * utf8 representation of the path components.
   * pathComponentBytes.length == components.
   * pathComponentBytes[i].length > 0
   */
  private byte[][] pathComponentBytes;
  /**
   * number of components in the full path.
   * components > 0
   */
  private int components;

  NamespaceInternalKey(final NamespaceKey path) {
    this(path, ENABLE_KEY_NORMALIZATION);
  }

  NamespaceInternalKey(final NamespaceKey path, boolean normalize) {
    this.namespaceKey = path;
    this.namespaceFullPath = path.getSchemaPath();
    this.keyBytes = null;

    final List<String> pathComponents = path.getPathComponents();
    this.components = pathComponents.size();

    if (components == 0) {
      throw UserException.validationError()
          .message("Invalid name space key. Given: %s, Expected format: %s", namespaceFullPath, NAMESPACE_PATH_FORMAT)
          .build(logger);
    }
    // Convert each path component into bytes.
    this.pathComponentBytes = new byte[components][];
    for (int i = 0; i < components; ++i) {
      if (pathComponents.get(i).length() == 0) {
        throw UserException.validationError()
            .message("Invalid name space key. Given: %s, Expected format: %s", namespaceFullPath, NAMESPACE_PATH_FORMAT)
            .build(logger);
      }
      if (normalize) {
        this.pathComponentBytes[i] = pathComponents.get(i).toLowerCase().getBytes(UTF_8);
      } else {
        this.pathComponentBytes[i] = pathComponents.get(i).getBytes(UTF_8);
      }
    }
  }

  private void buildKey() {
    if (keyBytes != null) {
      return;
    }
    /**
     * Worst case size for utf8 is 1-4 bytes.
     * Each component is prefixed with "delimiter-prefix-delimiter" of size {@link #DELIMITER_PREFIX_DELIMITER_SIZE}
     */
    this.keyBytes = new byte[4*namespaceFullPath.length() + components * DELIMITER_PREFIX_DELIMITER_SIZE];
    this.keyLength = 0;
    int count = pathComponentBytes.length - 1;
    for (int i = 0; i < components; ++i) {
      System.arraycopy(prefixes[count], 0, keyBytes, keyLength, prefixes[count].length);
      keyLength += prefixes[count].length;
      System.arraycopy(pathComponentBytes[i], 0, keyBytes, keyLength, pathComponentBytes[i].length);
      keyLength += pathComponentBytes[i].length;
      --count;
    }
  }

  public final byte[] getKey() {
    buildKey();
    if (cachedKey == null) {
      cachedKey = Arrays.copyOfRange(keyBytes, 0, keyLength);
    }
    return cachedKey;
  }

  private void buildRangeKeys() {
    /**
     * Worst case size for utf8 is 1-4 bytes.
     * Each component is prefixed with "delimiter-prefix-delimiter" of size {@link #DELIMITER_PREFIX_DELIMITER_SIZE}
     * Terminator
     */
    final byte [] rangeKeyBytes = new byte[
        4 * namespaceFullPath.length() +
        (components +1 )* DELIMITER_PREFIX_DELIMITER_SIZE +
        TERMINATOR.length
    ];

    int offset = 0;
    int count = components;
    for (int i = 0; i < components; ++i) {
      System.arraycopy(prefixes[count], 0, rangeKeyBytes, offset, prefixes[count].length);
      offset += prefixes[count].length;
      System.arraycopy(pathComponentBytes[i], 0, rangeKeyBytes, offset, pathComponentBytes[i].length);
      offset += pathComponentBytes[i].length;
      --count;
    }
    System.arraycopy(prefixes[0], 0, rangeKeyBytes, offset, prefixes[0].length);
    offset += prefixes[0].length;

    final byte[] tmpStartKey = Arrays.copyOfRange(rangeKeyBytes, 0, offset);
    final byte[] tmpEndKey = new byte[offset + TERMINATOR.length];
    System.arraycopy(rangeKeyBytes, 0, tmpEndKey, 0, offset);
    System.arraycopy(TERMINATOR, 0, tmpEndKey, offset, TERMINATOR.length);
    cachedRangeStartKey = tmpStartKey;
    cachedRangeEndKey = tmpEndKey;
  }

  public byte[] getRangeStartKey() {
    if (cachedRangeStartKey == null) {
      buildRangeKeys();
    }
    return cachedRangeStartKey;
  }

  public byte[] getRangeEndKey() {
    if (cachedRangeEndKey == null) {
      buildRangeKeys();
    }
    return cachedRangeEndKey;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((namespaceFullPath == null) ? 0 : namespaceFullPath.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    NamespaceInternalKey other = (NamespaceInternalKey) obj;
    if (namespaceFullPath == null) {
      if (other.namespaceFullPath != null) {
        return false;
      }
    } else if (!namespaceFullPath.equals(other.namespaceFullPath)) {
      return false;
    }
    return true;
  }

  public NamespaceKey getPath() {
    return namespaceKey;
  }

  @Override
  public String toString() {
    return namespaceFullPath;
  }

  //// Utilities.
  private static final byte[] rootLookupStartKey() {
    final byte[] start = new byte[DELIMITER_PREFIX_DELIMITER_SIZE + TERMINATOR.length];
    System.arraycopy(prefixes[0], 0, start, 0, prefixes[0].length);
    start[prefixes[0].length] = MIN_VALUE;
    return start;
  }

  private static final byte[] rootLookupEndKey() {
    final byte[] end = new byte[DELIMITER_PREFIX_DELIMITER_SIZE + TERMINATOR.length];
    System.arraycopy(prefixes[0], 0, end, 0, prefixes[0].length);
    end[prefixes[0].length] = MAX_VALUE;
    return end;
  }

  public static byte[] toPrefixBytes(int number) {
    final byte [] prefix = new byte[PREFIX_BYTES_SIZE];
    for (int i = PREFIX_BYTES_SIZE - 1; i >= 0; --i) {
      prefix[i] = (byte) (number & 0xFF);
      number >>>= 8;
    }
    return prefix;
  }

  public static int prefixBytesToInt(byte[] prefix) {
    int number = 0;
    for (int i = 0; i < PREFIX_BYTES_SIZE ; ++i) {
      number <<= 8;
      number ^= (prefix[i] & 0xFF);
    }
    return number;
  }

  // For testing/debugging purposes only. Don't use this method in production code.
  public static NamespaceInternalKey parseKey(byte[] keyBytes) {
    String path = extractKey(keyBytes, false);
    return new NamespaceInternalKey(new NamespaceKey(PathUtils.parseFullPath(path)));
  }

  // For testing/debugging purposes only. Don't use this method in production code.
  private static int indexOfDelimiterPrefixStart(byte[] array, int offset) {
    outer:
    for (int i = offset; i < array.length - DELIMITER_PREFIX_DELIMITER_SIZE + 1; i++) {
      for (int j = 0; j < DELIMITER_BYTES.length; j++) {
        if (array[i + j] != DELIMITER_BYTES[j]) {
          continue outer;
        }
      }
      // found the delimiter, now skip past the prefix number and then look for delimiter again
      final int newI = i + DELIMITER_BYTES.length + PREFIX_BYTES_SIZE;
      for(int j=0; j < DELIMITER_BYTES.length; j++) {
        if (array[newI + j] != DELIMITER_BYTES[j]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }

  // For testing/debugging purposes only. Don't use this method in production code.
  public static String extractKey(byte[] keyBytes, boolean includePrefixLevels) {
    final StringBuilder builder = new StringBuilder();

    int offset = indexOfDelimiterPrefixStart(keyBytes, 0);
    if (offset != 0) {
      throw new IllegalArgumentException("Key should start with delimiter-prefix-delimiter");
    }
    if (includePrefixLevels) {
      builder.append(extractLevelFromDelimiterPrefix(keyBytes, offset));
      builder.append(PATH_DELIMITER);
    }

    offset = offset + DELIMITER_PREFIX_DELIMITER_SIZE;
    while (true) {
      int index1 = indexOfDelimiterPrefixStart(keyBytes, offset);
      if (index1 == -1) {
        // Copy the remaining array as the last path component.
        builder.append(new String(keyBytes, offset, keyBytes.length - offset));
        break;
      } else {
        builder.append(new String(keyBytes, offset, index1 - offset));
        builder.append(PATH_DELIMITER);

        if (includePrefixLevels) {
          builder.append(extractLevelFromDelimiterPrefix(keyBytes, index1));
          builder.append(PATH_DELIMITER);
        }

        offset = index1 + DELIMITER_PREFIX_DELIMITER_SIZE;
      }
    }
    return builder.toString();
  }

  // For testing/debugging purposes only. Don't use this method in production code.
  public static String extractRangeKey(byte[] keyBytes) {
    final StringBuilder builder = new StringBuilder();

    int offset = indexOfDelimiterPrefixStart(keyBytes, 0);
    if (offset != 0) {
      throw new IllegalArgumentException("Key should start with delimiter-prefix-delimiter");
    }
    builder.append(extractLevelFromDelimiterPrefix(keyBytes, 0));
    builder.append(PATH_DELIMITER);

    offset = offset + DELIMITER_PREFIX_DELIMITER_SIZE;
    while (true) {
      int index1 = indexOfDelimiterPrefixStart(keyBytes, offset);
      if (index1 == -1) {
        break;
      }

      builder.append(new String(keyBytes, offset, index1 - offset));
      builder.append(PATH_DELIMITER);

      builder.append(extractLevelFromDelimiterPrefix(keyBytes, index1));
      builder.append(PATH_DELIMITER);

      offset = index1 + DELIMITER_PREFIX_DELIMITER_SIZE;
    }
    return builder.toString();
  }

  private static int extractLevelFromDelimiterPrefix(byte[] keyBytes, int delimiterStart) {
    return prefixBytesToInt(
        Arrays.copyOfRange(
            keyBytes,
            delimiterStart + DELIMITER_BYTES.length,
            delimiterStart + DELIMITER_BYTES.length + PREFIX_BYTES_SIZE
        )
    );
  }

  public static byte[] getRootLookupStart() {
    return ROOT_LOOKUP_START;
  }

  public static byte[] getRootLookupEnd() {
    return ROOT_LOOKUP_END;
  }
}
