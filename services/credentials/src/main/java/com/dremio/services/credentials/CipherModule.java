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

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.OptionalBinder;
import javax.inject.Provider;

/** A supplementary module for binding {@link Cipher} for system secrets. */
final class CipherModule extends AbstractModule {
  private final Provider<Cipher> cipher;

  CipherModule(Provider<Cipher> cipher) {
    this.cipher = cipher;
  }

  @Override
  protected void configure() {
    OptionalBinder.newOptionalBinder(binder(), Cipher.class).setBinding().toProvider(cipher);
  }
}
