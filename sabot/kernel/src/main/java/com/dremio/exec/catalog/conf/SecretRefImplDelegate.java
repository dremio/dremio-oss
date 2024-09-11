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

/**
 * A Protostuff Delegate to overwrite SecretRef ser/de logic. This serializes SecretRef as if it
 * were a string and vise versa on deserialization for backwards compatibility. Be aware Protostuff
 * delegates only operate on fields, this does not affect direct serializations of SecretRef.
 */
public final class SecretRefImplDelegate implements Delegate<SecretRef> {
  private static final Logger logger = LoggerFactory.getLogger(SecretRefImplDelegate.class);

  /**
   * Register this delegate to Protostuff RuntimeEnv if not already. Registration must happen before
   * any Schemas that contain SecretRef are created, otherwise the delegate will not apply (i.e.
   * during ConnectionReader creation).
   */
  public static void register(IdStrategy idStrategy) {
    // Register Delegate for custom ser/de logic
    if (!idStrategy.isDelegateRegistered(SecretRefImplDelegate.class)) {
      // registerDelegate is not available on the IdStrategy interface for some reason
      if (((DefaultIdStrategy) idStrategy).registerDelegate(new SecretRefImplDelegate())) {
        logger.debug("SecretRefDelegate registered to DefaultIdStrategy");
      }
    }
  }

  @Override
  public WireFormat.FieldType getFieldType() {
    return WireFormat.FieldType.STRING;
  }

  @Override
  public SecretRef readFrom(Input input) throws IOException {
    return SecretRef.of(input.readString());
  }

  @SuppressForbidden // We reference SecretRefUnsafe to explicitly block it from serialization
  @Override
  public void writeTo(Output output, int number, SecretRef value, boolean repeated)
      throws IOException {
    if (value == null) {
      return;
    }
    final String stringValue;
    if (SecretRef.EMPTY.equals(value)) {
      stringValue = "";
    } else if (SecretRef.EXISTING_VALUE.equals(value)) {
      stringValue = ConnectionConf.USE_EXISTING_SECRET_VALUE;
    } else if (value instanceof SecretRefUnsafe) {
      throw new IllegalArgumentException(
          String.format("Cannot serialize value of type '%s'", SecretRefUnsafe.class.getName()));
    } else if (value instanceof AbstractSecretRef) {
      stringValue = ((AbstractSecretRef) value).getRaw();
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot serialize value of type '%s'", value.getClass().getName()));
    }
    output.writeString(number, stringValue, repeated);
  }

  @Override
  public void transfer(Pipe pipe, Input input, Output output, int number, boolean repeated)
      throws IOException {
    input.transferByteRangeTo(output, true, number, repeated);
  }

  @Override
  public Class<?> typeClass() {
    return SecretRef.class;
  }
}
