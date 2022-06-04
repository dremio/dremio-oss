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
package com.dremio.exec.store;

import com.dremio.exec.catalog.CatalogIdentity;

public class AuthorizationContext {
  private final CatalogIdentity subject;
  private final boolean ignoreAuthErrors;

  public AuthorizationContext(final CatalogIdentity subject, final boolean ignoreAuthErrors) {
    this.subject = subject;
    this.ignoreAuthErrors = ignoreAuthErrors;
  }

  public CatalogIdentity getSubject() {
    return subject;
  }

  public boolean getIgnoreAuthErrors() {
    return ignoreAuthErrors;
  }
}
