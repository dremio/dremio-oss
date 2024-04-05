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

import com.dremio.common.SuppressForbidden;
import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Pipe;
import io.protostuff.WireFormat;
import io.protostuff.runtime.DefaultIdStrategy;
import io.protostuff.runtime.Delegate;
import io.protostuff.runtime.IdStrategy;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressForbidden
public class SecretRefUnsafeDelegate implements Delegate<SecretRefUnsafe> {
  private static final Logger logger = LoggerFactory.getLogger(SecretRefUnsafeDelegate.class);

  /**
   * Register this delegate to Protostuff RuntimeEnv if not already. Registration must happen before
   * any Schemas that contain SecretRef are created, otherwise the delegate will not apply (i.e.
   * during ConnectionReader creation).
   */
  public static void register(IdStrategy idStrategy) {
    // Register Delegate for custom ser/de logic
    if (!idStrategy.isDelegateRegistered(SecretRefUnsafeDelegate.class)) {
      // registerDelegate is not available on the IdStrategy interface for some reason
      if (((DefaultIdStrategy) idStrategy).registerDelegate(new SecretRefUnsafeDelegate())) {
        logger.debug("SecretRefDelegate registered to DefaultIdStrategy");
      }
    }
  }

  @Override
  public WireFormat.FieldType getFieldType() {
    return WireFormat.FieldType.STRING;
  }

  @Override
  public SecretRefUnsafe readFrom(Input input) throws IOException {
    throw new UnsupportedOperationException("Deserialization of unsafe secret ref not permitted");
  }

  @Override
  public void writeTo(Output output, int number, SecretRefUnsafe value, boolean repeated)
      throws IOException {
    throw new UnsupportedOperationException("Serialization of unsafe secret ref not permitted");
  }

  @Override
  public void transfer(Pipe pipe, Input input, Output output, int number, boolean repeated)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Class<?> typeClass() {
    return SecretRefUnsafe.class;
  }
}
