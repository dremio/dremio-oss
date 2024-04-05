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

/** CredentialsProvider operations */
public interface CredentialsProvider {

  /**
   * Returns the secret from the pattern.
   *
   * @return a string represents the secret indicated by the pattern
   * @throws IllegalArgumentException if {@code pattern} is invalid
   * @throws CredentialsException if {@code pattern} encounters internal or IO error
   */
  String lookup(URI pattern) throws IllegalArgumentException, CredentialsException;

  /** Returns true if a URI pattern is supported. */
  boolean isSupported(URI pattern);
}
