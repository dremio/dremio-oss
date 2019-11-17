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
package com.dremio.security;

import java.io.IOException;
import java.net.URI;

/**
 * Interface for various credential types to support.
 */
public interface Vault {

  /**
   * Gets the credentials specified by given secretURI. If api cannot fetch the
   * credentials for any reason, it would throw IOException and would not
   * return null.
   *
   * @param secretURI URI with location and query params.
   * @return credentials specified by secretURI
   * @throws IOException
   */
  Credentials getCredentials(URI secretURI) throws IOException;
}
