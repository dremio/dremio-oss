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

import com.dremio.service.Service;
import java.net.URI;

/** CredentialsService operations */
public interface CredentialsService extends Service {

  /** Check if given URI can be resolved by a provider in this CredentialsService */
  boolean isSupported(URI uri);

  /**
   * Returns the secret from the pattern through loaded providers
   *
   * @return a string represents the secret indicated by the pattern
   */
  String lookup(String pattern) throws IllegalArgumentException, CredentialsException;
}
