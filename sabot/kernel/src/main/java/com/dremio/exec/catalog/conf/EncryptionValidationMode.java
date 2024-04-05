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
package com.dremio.exec.catalog.conf;

import io.protostuff.Tag;

/** Enumerates supported encryption verification modes used by most sources */
public enum EncryptionValidationMode {
  @Tag(1)
  @DisplayMetadata(label = "Validate certificate and hostname")
  CERTIFICATE_AND_HOSTNAME_VALIDATION,

  @Tag(2)
  @DisplayMetadata(label = "Validate certificate only")
  CERTIFICATE_ONLY_VALIDATION,

  @Tag(3)
  @DisplayMetadata(label = "Do not validate certificate or hostname")
  NO_VALIDATION
}
