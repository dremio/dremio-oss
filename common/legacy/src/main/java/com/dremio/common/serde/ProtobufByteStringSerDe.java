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
package com.dremio.common.serde;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;

/** Utility class to serialize/deserialize objects to/from {@link ByteString}. */
public final class ProtobufByteStringSerDe {

  /** Interface to compress/decompress data to ByteString */
  public interface Codec {
    /** No compression */
    public static final Codec NONE =
        new Codec() {
          @Override
          public OutputStream compress(OutputStream output) {
            return output;
          }

          @Override
          public InputStream decompress(InputStream input) {
            return input;
          }
        };

    /**
     * Wrap the provided output for compression
     *
     * @param output the output where to write the compressed data
     * @return an {@code OutputStream} instance where to write data to compress
     */
    OutputStream compress(OutputStream output) throws IOException;

    /**
     * Wrap the provided input for decompression
     *
     * @param input to use to read compressed data
     * @return an {@code InputStream} to read data uncompressed
     */
    InputStream decompress(InputStream input) throws IOException;
  }

  /**
   * Serialize the given value to byte string using the given mapper, employing the given codec
   * algorithm.
   *
   * @param mapper object mapper
   * @param value value to serialize
   * @param codec codec
   * @return serialized bytes
   * @throws JsonGenerationException in case of serialization errors
   */
  public static ByteString writeValue(ObjectMapper mapper, Object value, Codec codec)
      throws JsonGenerationException {
    final Output output = ByteString.newOutput();

    try {
      final OutputStream os = codec.compress(output);
      try {
        mapper.writer().without(SerializationFeature.INDENT_OUTPUT).writeValue(os, value);
      } finally {
        os.close();
      }
    } catch (IOException e) {
      // Should not happen but...
      throw new JsonGenerationException(e, null);
    }

    // Javadoc says data is copied, but it's more of a transfer of ownership!
    return output.toByteString();
  }

  /**
   * Deserialize the given byte string to an object using the given reader, employing the given
   * codec algorithm.
   *
   * @param reader object reader
   * @param byteString byte string to deserialize
   * @param codec codec
   * @param logger logger
   * @param <T> object type of the deserialized object
   * @return deserialized object
   * @throws IOException in case of deserialization errors
   */
  public static <T> T readValue(
      ObjectReader reader, ByteString byteString, Codec codec, Logger logger) throws IOException {

    if (logger.isTraceEnabled()) {
      // Costly conversion to UTF-8. Avoid if possible
      ByteSource bs =
          new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
              return codec.decompress(byteString.newInput());
            }
          };
      final String value = bs.asCharSource(StandardCharsets.UTF_8).read();
      logger.trace("Attempting to read {}", value);

      // Could reuse the value but to avoid so that logger level doesn't impact the program flow
      // Since level is trace, user probably doesn't care for performance right now
      // return reader.readValue(value);
    }

    try (InputStream is = codec.decompress(byteString.newInput())) {
      return reader.readValue(is);
    }
  }

  private ProtobufByteStringSerDe() {}
}
