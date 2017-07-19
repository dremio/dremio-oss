/*
 * Copyright (C) 2017 Dremio Corporation
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

/**
 * Namespace specific exception.
 */
public class NamespaceNotFoundException extends NamespaceException {
  private static final long serialVersionUID = 1L;

  private final NamespaceKey key;

  public NamespaceNotFoundException(NamespaceKey key, String message) {
    super(message + ": " + key.toString());
    this.key = key;
  }

  public NamespaceKey getKey() {
    return key;
  }
}
