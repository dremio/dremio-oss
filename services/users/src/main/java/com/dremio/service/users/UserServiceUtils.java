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
package com.dremio.service.users;

import com.dremio.datastore.SearchTypes;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;
import javax.crypto.SecretKeyFactory;

/** Common utilities for UserService */
public class UserServiceUtils {

  public static final Pattern PASSWORD_MATCHER = Pattern.compile("(?=.*[0-9])(?=.*[a-zA-Z]).{8,}");
  public static final SearchTypes.SearchFieldSorting DEFAULT_SORTER =
      UserIndexKeys.NAME.toSortField(SearchTypes.SortOrder.ASCENDING);
  public static final String USER_AUTHENTICATION_ERROR_MESSAGE =
      "Login failed: Invalid username or password";

  public static SecretKeyFactory buildSecretKey() {
    try {
      return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
    } catch (NoSuchAlgorithmException nsae) {
      throw new RuntimeException("Failed to initialize usergroup service", nsae);
    }
  }

  public static boolean validateUsername(String input) throws IllegalArgumentException {
    // DX-8156: These two characters `":` currently cause trouble, particularly for constructing SQL
    // queries.
    return input != null
        && !input.isEmpty()
        && !input.contains(String.valueOf('"'))
        && !input.contains(":");
  }

  public static boolean validatePassword(String input) throws IllegalArgumentException {
    return input != null && !input.isEmpty() && PASSWORD_MATCHER.matcher(input).matches();
  }

  public static boolean slowEquals(byte[] a, byte[] b) {
    int diff = a.length ^ b.length;
    for (int i = 0; i < a.length && i < b.length; i++) {
      diff |= a[i] ^ b[i];
    }
    return diff == 0;
  }
}
