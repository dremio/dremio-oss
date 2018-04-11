/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.logical.serialization.serializers;

import java.net.URI;
import java.net.URISyntaxException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


/**
 * Holds serializers for standard java objects.
 */
public final class JavaSerializers {

  public static void register(final Kryo kryo) {
    kryo.addDefaultSerializer(URI.class, URISerializer.class);
  }

  public static class URISerializer extends Serializer<URI> {

    @Override
    public void write(final Kryo kryo, final Output output, final URI object) {
      final String uriString = object.toString();
      kryo.writeObject(output, uriString);
    }

    @Override
    public URI read(final Kryo kryo, final Input input, final Class<URI> type) {
      final String uriString = kryo.readObject(input, String.class);
      try {
        return new URI(uriString);
      } catch (final URISyntaxException e) {
        throw new RuntimeException(String.format("unable to deserialize URI from uri string: %s", uriString), e);
      }
    }
  }
}
