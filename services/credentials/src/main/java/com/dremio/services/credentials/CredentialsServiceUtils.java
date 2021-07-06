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
package com.dremio.services.credentials;

import java.net.URI;

/**
 * Utility methods for credentials service.
 */
public final class CredentialsServiceUtils {

  /**
   * Create a URI from a String. If the String is not a valid URI, the exception thrown will not contain the original string.
   *
   * @param pattern the string to create URI
   * @return URI created
   * @throws IllegalArgumentException
   */
  public static URI safeURICreate(String pattern) {
    try {
      return URI.create(pattern);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("The provided string is not a valid URI.");
    }
  }

  // prevent instantiation
  private CredentialsServiceUtils() {
  }

}
