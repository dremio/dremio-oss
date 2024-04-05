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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.function.Supplier;

/**
 * A container for a secret. Useful to receives custom Protostuff ser/de logic and ensure compliance
 * with source-level, in-line secret encryption.
 */
@FunctionalInterface
@JsonDeserialize(as = SecretRefImpl.class)
public interface SecretRef extends Supplier<String> {
  /**
   * The empty instance, acts roughly equivalent to an empty string. Allowed to freely retrieve the
   * empty value.
   */
  SecretRef EMPTY =
      new SecretRef() {
        @Override
        public String get() {
          return "";
        }

        @Override
        public boolean equals(Object obj) {
          if (obj instanceof AbstractSecretRef) {
            return ((AbstractSecretRef) obj).getRaw().isEmpty();
          } else if (obj instanceof SecretRef) {
            return ((SecretRef) obj).get().isEmpty();
          }
          return super.equals(obj);
        }

        @Override
        public int hashCode() {
          return "".hashCode();
        }
      };

  SecretRef EXISTING_VALUE =
      new SecretRef() {
        @Override
        public String get() {
          return ConnectionConf.USE_EXISTING_SECRET_VALUE;
        }

        @Override
        public boolean equals(Object obj) {
          if (obj instanceof AbstractSecretRef) {
            return ConnectionConf.USE_EXISTING_SECRET_VALUE.equals(
                ((AbstractSecretRef) obj).getRaw());
          } else if (obj instanceof SecretRef) {
            return ConnectionConf.USE_EXISTING_SECRET_VALUE.equals(((SecretRef) obj).get());
          }
          return super.equals(obj);
        }

        @Override
        public int hashCode() {
          return ConnectionConf.USE_EXISTING_SECRET_VALUE.hashCode();
        }
      };

  /** Get the resolved Secret */
  @Override
  String get();

  /** Is the SecretRef empty */
  static boolean isEmpty(SecretRef secretRef) {
    return secretRef.equals(EMPTY);
  }

  /** Get an empty SecretRef */
  static SecretRef empty() {
    return EMPTY;
  }

  /** Is the SecretRef null or empty */
  static boolean isNullOrEmpty(SecretRef secretRef) {
    return secretRef == null || isEmpty(secretRef);
  }

  /** Create a SecretRef out of given secret */
  static SecretRef of(String secret) {
    if (secret == null) {
      return null;
    } else if (secret.isEmpty()) {
      return EMPTY;
    }
    return new SecretRefImpl(secret);
  }
}
