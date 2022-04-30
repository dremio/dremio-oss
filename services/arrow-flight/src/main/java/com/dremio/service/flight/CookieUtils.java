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
package com.dremio.service.flight;

import java.util.Iterator;

import org.apache.arrow.flight.CallHeaders;

/**
 * Utilities for parsing cookies
 */
public class CookieUtils {
  public static final String COOKIE_HEADER = "Cookie";

  /**
   * Returns the first value found for a cookie
   *
   * @param callHeaders headers to look through
   * @param cookieName name of cookie to look for
   * @return the value of the cookie if found or null
   */
  public static String getCookieValue(CallHeaders callHeaders, String cookieName) {
    final Iterable<String> cookieHeaderValues = callHeaders.getAll(COOKIE_HEADER);
    if (cookieHeaderValues == null) {
      return null;
    }

    for (Iterator<String> headerIterator = cookieHeaderValues.iterator(); headerIterator.hasNext();) {
      final String headerValue = headerIterator.next();

      final String[] cookies = headerValue.split(";");
      for (String cookie : cookies) {
        final int equalsIndex = cookie.indexOf('=');
        if (equalsIndex > 0 && cookieName.equals(cookie.substring(0, equalsIndex).trim())) {
          return cookie.substring(equalsIndex + 1).trim();
        }
      }
    }

    return null;
  }
}
