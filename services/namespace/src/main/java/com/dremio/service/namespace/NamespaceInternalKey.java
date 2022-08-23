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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.google.common.base.Strings;

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

  // TODO: make it independent of SQL
  // Back ticks are allowed to escape dot/keywords.
  private static final char QUOTE = SqlUtils.QUOTE;

  // The delimiter is "``" in UTF-8.
  static final byte[] DELIMITER_BYTES = format("%c%c", QUOTE, QUOTE).getBytes(UTF_8);

  static final char PATH_DELIMITER = '.';

  static final int PREFIX_BYTES_SIZE = 2;
  static final int DELIMITER_PREFIX_DELIMITER_BYTES_SIZE =
    DELIMITER_BYTES.length + PREFIX_BYTES_SIZE + DELIMITER_BYTES.length;

  // 2 bytes are allocated for encoding each prefix level.
  // The largest valid byte value in UTF-8 is 127.
  // By supporting prefix levels from 0 to 127 (128 prefixes)
  // Since one prefix should be saved for encoding range keys,
  // the max number of path components we can support is 127.
  private static final int MAX_NUM_PATH_COMPONENTS = 127;

  private static final byte MASK = (byte) 0xFF;

  // Max and Min values for building root lookup keys.

  // The largest value in Unicode Plane 0 aka Basic Multilingual Plane (BMP).
  // This value is used as a terminator for end range keys.
  static final byte[] MAX_UTF8_VALUE = new byte[]{(byte) 0xef, (byte) 0xbf, (byte) 0xbf}; //U+FFFF

  // The smallest valid value in UTF-8. This value is used as a terminator for start range keys.
  private static final byte[] MIN_UTF8_VALUE = new byte[]{(byte) 0x0};

  // Root lookup keys for FindByRange searches.
  private static final String ROOT_LOOKUP_START_KEY = generateRootLookupStartKey();
  private static final String ROOT_LOOKUP_END_KEY = generateRootLookupEndKey();

  private static final String ERROR_MSG_EXPECTED_NAMESPACE_PATH_FORMAT =
    "Namespace path should be of format <space>.<folder1>.<folder2>....<folderN>.<dataset/file>";

  /**
   * Creates a root lookup start key.
   * @return root lookup start key.
   */
  private static String generateRootLookupStartKey() {
    return rootLookupKeyHelper(MIN_UTF8_VALUE);
  }

  /**
   * Creates a root lookup end key.
   * @return root lookup end key.
   */
  private static String generateRootLookupEndKey() {
    return rootLookupKeyHelper(MAX_UTF8_VALUE);
  }


  private static final List<String> PREFIXES = generatePrefixes();

  /**
   * Generates a given number of PREFIXES. It only generates up to 0x7f since all values after this
   * is invalid in UTF-8.
   * @return an array of byte[] PREFIXES.
   */
  private static List<String> generatePrefixes() {
    final List<String> prefixes = new ArrayList<>();
    for (int i = 0; i <= MAX_NUM_PATH_COMPONENTS; ++i) {
      prefixes.add(new String(prefixWithDelimiters(i), StandardCharsets.UTF_8));
    }
    return prefixes;
  }

  private final String key;
  private final String rangeStartKey;
  private final String rangeEndKey;
  private final String namespaceFullPath;
  private final NamespaceKey namespaceKey;

  NamespaceInternalKey(final NamespaceKey path) {
    this(path, ENABLE_KEY_NORMALIZATION);
  }

  NamespaceInternalKey(final NamespaceKey path, boolean normalize) {
    final List<String> processedPathComponents = processPathComponents(path, normalize);
    this.key = buildKey(processedPathComponents);
    this.rangeStartKey = buildRangeStartKey(processedPathComponents);
    this.rangeEndKey = buildRangeEndKey(rangeStartKey);
    this.namespaceKey = path;
    this.namespaceFullPath = path.getSchemaPath();
  }

  String getKey() {
    return key;
  }

  NamespaceKey getPath() {
    return namespaceKey;
  }

  String getRangeStartKey() {
    return rangeStartKey;
  }

  String getRangeEndKey() {
    return rangeEndKey;
  }

  static String getRootLookupStartKey() {
    return ROOT_LOOKUP_START_KEY;
  }

  static String getRootLookupEndKey() {
    return ROOT_LOOKUP_END_KEY;
  }

  /**
   * Processes the provided NamespaceKey's path components.
   * Converts each path component to lower case if {@param normalize} is true.
   * Returns a list of path components.
   *
   * @param path the NamespaceKey to process.
   * @param normalize indicates whether path components should be converted to lower case.
   * @return a list of path components.
   */
  static List<String> processPathComponents(final NamespaceKey path, boolean normalize) {
    final List<String> processedPathComponents = new ArrayList<>();
    final int numPathComponents = path.getPathComponents().size();

    if(numPathComponents == 0) {
      throw UserException.validationError().message("Invalid name space key. Given: %s, Expected format: %s",
        path.getSchemaPath(), ERROR_MSG_EXPECTED_NAMESPACE_PATH_FORMAT).build(logger);
    } else if (numPathComponents > MAX_NUM_PATH_COMPONENTS) {
      throw UserException.unsupportedError().message("Provided file path has too many components.")
        .addContext("File Path: ", path.getSchemaPath()).build(logger);
    }
    path.getPathComponents().forEach(component -> {
      if(Strings.isNullOrEmpty(component)) {
        throw UserException.validationError().message("Invalid name space key. Given: %s, Expected format: %s",
          path.getSchemaPath(), ERROR_MSG_EXPECTED_NAMESPACE_PATH_FORMAT).build(logger);
      }
      processedPathComponents.add((normalize)? component.toLowerCase(Locale.ROOT) : component);
    });

    return processedPathComponents;
  }

  /**
   * Builds a NamespaceInternalKey given a list of String path components.
   *
   * @param pathComponents the list of string path components.
   * @return a String key created with the list of path components.
   */
  private String buildKey(List<String> pathComponents) {
    return buildKeyWithPrefixes(pathComponents, false);
  }

  /**
   * Builds the range start key provided a list of path components.
   * Used in FindByRange as the start range key.
   *
   * @param pathComponents a list of String path components.
   * @return range start key.
   */
  private String buildRangeStartKey(List<String> pathComponents) {
    return buildKeyWithPrefixes(pathComponents, true);
  }

  /**
   * Builds a range end key provided a rangeStartKey. Appends byte terminator to the end of rangeStartKey.
   * Used in FindByRange as the end range key.
   *
   * @param rangeStartKey rangeStartKey.
   * @return range end key.
   */
  private String buildRangeEndKey(String rangeStartKey) {
    return rangeStartKey + new String(MAX_UTF8_VALUE, StandardCharsets.UTF_8);
  }


  /**
   * Helper to build a String key given a list of path components.
   * Either builds the key or a range key.
   *
   * @param pathComponents a list of String path components.
   * @param isRangeKey if true, builds a range key, otherwise builds the key.
   * @return a String key that is either the key or a range key.
   */
  private String buildKeyWithPrefixes(List<String> pathComponents, boolean isRangeKey) {
    final StringBuilder keyBuilder = new StringBuilder(calculateKeyLength(pathComponents, isRangeKey));
    int prefixIndex = pathComponents.size() - (isRangeKey ?  0 : 1);
    for (int i = 0; i < pathComponents.size(); i++) {
      keyBuilder.append(PREFIXES.get(prefixIndex));
      keyBuilder.append(pathComponents.get(i));
      --prefixIndex;
    }
    if (isRangeKey) {
      keyBuilder.append(PREFIXES.get(prefixIndex));
    }
    return keyBuilder.toString();
  }

  /**
   * Calculates the length of the key given its path components and key type.
   *
   * @param pathComponents the path components
   * @param isRangeKey indicates whether range key length or key length is being calculated.
   * @return the length as an integer.
   */
  private int calculateKeyLength(List<String> pathComponents, boolean isRangeKey) {
    int length = 0;
    int prefixIndex = pathComponents.size() - (isRangeKey ?  0 : 1);
    for (int i = 0; i < pathComponents.size(); i++) {
      length += pathComponents.get(i).length();
      length += PREFIXES.get(prefixIndex).length();
      prefixIndex--;
    }
    return length + (isRangeKey ? PREFIXES.get(prefixIndex).length() : 0);
  }

  /**
   * Writes a 2-byte representation of an integer to the given byte[].
   *
   * @param dst the destination byte[].
   * @param offset the offset to write bytes at.
   * @param number the number to convert into 2-bytes unsigned.
   */
  private static void writePrefixLevel(byte[] dst, int offset, int number) {
    dst[offset + 1] = (byte) (number & MASK);
    number >>>= 8;
    dst[offset] = (byte) (number & MASK);
  }

  /**
   * Creates a prefix byte sequence in the following format
   * delimiter - prefix level - delimiter
   *
   * @param prefix the integer prefix
   * @return the prefix and delimiters byte sequence.
   */
  private static byte[] prefixWithDelimiters(int prefix) {
    final byte[] bytes = new byte[DELIMITER_PREFIX_DELIMITER_BYTES_SIZE];
    System.arraycopy(DELIMITER_BYTES, 0, bytes, 0, DELIMITER_BYTES.length);
    writePrefixLevel(bytes, DELIMITER_BYTES.length, prefix);
    System.arraycopy(DELIMITER_BYTES, 0, bytes, DELIMITER_BYTES.length + PREFIX_BYTES_SIZE, DELIMITER_BYTES.length);
    return bytes;
  }

  /**
   * Helper to create a root lookup key with the provided terminator.
   *
   * @param terminator the terminator for this key.
   * @return a root lookup key suffixed with the provided terminator.
   */
  private static String rootLookupKeyHelper(byte[] terminator) {
    final byte[] key = new byte[DELIMITER_PREFIX_DELIMITER_BYTES_SIZE + terminator.length];
    System.arraycopy(prefixWithDelimiters(0), 0, key, 0, DELIMITER_PREFIX_DELIMITER_BYTES_SIZE);
    System.arraycopy(terminator, 0, key, DELIMITER_PREFIX_DELIMITER_BYTES_SIZE, terminator.length);
    return new String (key, StandardCharsets.UTF_8);
  }

  @Override
  public String toString() {
    return namespaceFullPath;
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
}
