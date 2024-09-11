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

package com.dremio.dac.datatype;

import static com.dremio.dac.datatype.ScaledBytes.Scale.BYTES;
import static com.dremio.dac.datatype.ScaledBytes.Scale.GIGABYTES;
import static com.dremio.dac.datatype.ScaledBytes.Scale.KILOBYTES;
import static com.dremio.dac.datatype.ScaledBytes.Scale.MEGABYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class TestScaledBytes {

  private static final String NOT_A_LONG = "is either out-of-range or not parseable as a long";
  private static final String BAD_UNIT = "is not a supported unit";
  private static final String MATCH_FAILURE = "which does not match this pattern";

  /**
   * Convert quantity {@code sourceValue} with scale {@code sourceScale} to scale {@code
   * targetScale}, expecting the resulting quantity to be {@code targetValue}.
   */
  @ParameterizedTest
  @MethodSource("argsScaleConversions")
  public void testScaleConversions(
      final long sourceValue,
      final ScaledBytes.Scale sourceScale,
      final long targetValue,
      final ScaledBytes.Scale targetScale) {
    final ScaledBytes sb = ScaledBytes.of(sourceValue, sourceScale);
    assertThat(sb.getQuantity(targetScale)).isEqualTo(targetValue);
  }

  private static Stream<Arguments> argsScaleConversions() {
    return Stream.of(
        Arguments.of(999, KILOBYTES, 0, MEGABYTES),
        Arguments.of(1000, KILOBYTES, 0, MEGABYTES),
        Arguments.of(1001, KILOBYTES, 0, MEGABYTES),
        Arguments.of(1023, KILOBYTES, 0, MEGABYTES),
        Arguments.of(1024, KILOBYTES, 1, MEGABYTES),
        Arguments.of(1025, KILOBYTES, 1, MEGABYTES),
        Arguments.of(1L << 50, KILOBYTES, 1152921504606846976L, BYTES),
        Arguments.of(0, BYTES, 0, MEGABYTES),
        Arguments.of(1, BYTES, 0, MEGABYTES),
        Arguments.of(1025, BYTES, 0, MEGABYTES),
        Arguments.of(1048575, BYTES, 0, MEGABYTES),
        Arguments.of(1048576, BYTES, 1, MEGABYTES),
        Arguments.of(1048577, BYTES, 1, MEGABYTES),
        Arguments.of(5000000, BYTES, 4, MEGABYTES),
        Arguments.of(1, GIGABYTES, 1024, MEGABYTES),
        Arguments.of(1, GIGABYTES, 1048576, KILOBYTES),
        Arguments.of(1, GIGABYTES, 1073741824, BYTES),
        Arguments.of(200, GIGABYTES, 204800L, MEGABYTES),
        Arguments.of(200, GIGABYTES, 209715200L, KILOBYTES),
        Arguments.of(200, GIGABYTES, 214748364800L, BYTES),
        Arguments.of(1L << 30, GIGABYTES, 1152921504606846976L, BYTES),
        Arguments.of(0, GIGABYTES, 0, BYTES),
        Arguments.of(0, GIGABYTES, 0, KILOBYTES),
        Arguments.of(0, GIGABYTES, 0, MEGABYTES),
        Arguments.of(0, GIGABYTES, 0, GIGABYTES),
        Arguments.of(0, BYTES, 0, BYTES),
        Arguments.of(0, BYTES, 0, KILOBYTES),
        Arguments.of(0, BYTES, 0, MEGABYTES),
        Arguments.of(0, BYTES, 0, GIGABYTES),
        Arguments.of(1L << 40, MEGABYTES, 1152921504606846976L, BYTES),
        Arguments.of(1234567, MEGABYTES, 1234567, MEGABYTES));
  }

  @ParameterizedTest
  @MethodSource("argsUnsupportedQuantities")
  public void testUnsupportedQuantities(final long quantity, final ScaledBytes.Scale scale) {
    assertThatThrownBy(() -> ScaledBytes.of(quantity, scale))
        .hasMessageContaining("exceeds the supported range");
  }

  private static Stream<Arguments> argsUnsupportedQuantities() {
    return Stream.of(
        Arguments.of((1L << 30) + 1L, GIGABYTES),
        Arguments.of(1L << 31, GIGABYTES),
        Arguments.of(1L << 32, GIGABYTES),
        Arguments.of((1L << 40) + 1L, MEGABYTES),
        Arguments.of(1L << 41, MEGABYTES),
        Arguments.of((1L << 50) + 1L, KILOBYTES),
        Arguments.of(1L << 51, KILOBYTES),
        Arguments.of((1L << 60) + 1L, BYTES),
        Arguments.of(1L << 61, BYTES),
        Arguments.of(-1L, BYTES),
        Arguments.of(-1L, KILOBYTES),
        Arguments.of(-1L, MEGABYTES),
        Arguments.of(-1L, GIGABYTES));
  }

  @ParameterizedTest
  @MethodSource("argsDeserializationSuccess")
  public void testDeserializationSuccess(final String input, final ScaledBytes expected)
      throws IOException {
    ScaledBytesDeserializer des = ScaledBytesDeserializer.create();

    JsonParser mockJsonParser = mock(JsonParser.class);
    DeserializationContext mockDeserializationContext = mock(DeserializationContext.class);
    when(mockJsonParser.readValueAs(eq(String.class))).thenReturn(input);

    ScaledBytes output = des.deserialize(mockJsonParser, mockDeserializationContext);

    assertThat(output).isEqualTo(expected);
    verify(mockJsonParser).readValueAs(eq(String.class));
    verifyNoMoreInteractions(mockJsonParser);
    verifyNoMoreInteractions(mockDeserializationContext);
  }

  private static Stream<Arguments> argsDeserializationSuccess() {
    return Stream.of(
        Arguments.of("0000 b", ScaledBytes.of(0, BYTES)),
        Arguments.of("0000 B", ScaledBytes.of(0, BYTES)),
        Arguments.of("0 B", ScaledBytes.of(0, BYTES)),
        Arguments.of("2 b", ScaledBytes.of(2, BYTES)),
        Arguments.of("1023 B", ScaledBytes.of(1023, BYTES)),
        Arguments.of("1024 b", ScaledBytes.of(1024, BYTES)),
        Arguments.of("1025 b", ScaledBytes.of(1025, BYTES)),
        Arguments.of("01010 B", ScaledBytes.of(1010, BYTES)),
        Arguments.of("0 Kb", ScaledBytes.of(0, KILOBYTES)),
        Arguments.of("4000 kB", ScaledBytes.of(4000, KILOBYTES)),
        Arguments.of("4096 KB", ScaledBytes.of(4096, KILOBYTES)),
        Arguments.of("0567 kb", ScaledBytes.of(567, KILOBYTES)),
        Arguments.of("2048 MB ", ScaledBytes.of(2048, MEGABYTES)),
        Arguments.of("40000000 mb ", ScaledBytes.of(40_000_000, MEGABYTES)),
        Arguments.of("0 Gb", ScaledBytes.of(0, GIGABYTES)),
        Arguments.of("1 gb", ScaledBytes.of(1, GIGABYTES)),
        Arguments.of("16 gB", ScaledBytes.of(16, GIGABYTES)),
        Arguments.of("00000000000000000000000000000000000002 gb", ScaledBytes.of(2, GIGABYTES)),
        Arguments.of("6000000 GB", ScaledBytes.of(6_000_000, GIGABYTES)),
        Arguments.of(
            "                             6000000 GB                                             ",
            ScaledBytes.of(6_000_000, GIGABYTES)));
  }

  @ParameterizedTest
  @MethodSource("argsDeserializationFailure")
  public void testDeserializationFailure(final String input, final String exceptionMessageFragment)
      throws IOException {
    ScaledBytesDeserializer des = ScaledBytesDeserializer.create();

    JsonParser mockJsonParser = mock(JsonParser.class);
    DeserializationContext mockDeserializationContext = mock(DeserializationContext.class);
    when(mockJsonParser.readValueAs(eq(String.class))).thenReturn(input);

    assertThatThrownBy(() -> des.deserialize(mockJsonParser, mockDeserializationContext))
        .isInstanceOf(InvalidFormatException.class)
        .hasMessageContaining(exceptionMessageFragment);

    verify(mockJsonParser).readValueAs(eq(String.class));
    verifyNoMoreInteractions(mockJsonParser);
    verifyNoMoreInteractions(mockDeserializationContext);
  }

  private static Stream<Arguments> argsDeserializationFailure() {
    return Stream.of(
        Arguments.of("200000000000000000000000000000000000002 gb", NOT_A_LONG),
        Arguments.of("-1 gb", MATCH_FAILURE),
        Arguments.of("1 gbgb", BAD_UNIT),
        Arguments.of("1 gkb", BAD_UNIT),
        Arguments.of("1 PB", BAD_UNIT),
        Arguments.of("1 gb kb", MATCH_FAILURE),
        Arguments.of("leading_garbage 1 gb trailing_garbage", MATCH_FAILURE),
        Arguments.of("leading_garbage 1 kb", MATCH_FAILURE),
        Arguments.of("1 b trailing_garbage", MATCH_FAILURE),
        Arguments.of("kb 1", MATCH_FAILURE),
        Arguments.of("1", MATCH_FAILURE),
        Arguments.of("The quick brown fox jumps over the lazy dog", MATCH_FAILURE));
  }

  @ParameterizedTest
  @MethodSource("argsSerialization")
  public void testSerialization(
      final long quantity, final ScaledBytes.Scale scale, final String expected)
      throws IOException {
    ScaledBytesSerializer ser = ScaledBytesSerializer.create();
    ScaledBytes value = ScaledBytes.of(quantity, scale);

    JsonGenerator mockJsonGenerator = mock(JsonGenerator.class);
    SerializerProvider mockSerializerProvider = mock(SerializerProvider.class);
    ArgumentCaptor<String> outputCaptor = ArgumentCaptor.forClass(String.class);

    ser.serialize(value, mockJsonGenerator, mockSerializerProvider);

    verify(mockJsonGenerator).writeString(outputCaptor.capture());
    assertThat(outputCaptor.getValue()).isEqualTo(expected);
    verifyNoMoreInteractions(mockJsonGenerator);
    verifyNoMoreInteractions(mockSerializerProvider);
  }

  private static Stream<Arguments> argsSerialization() {
    return Stream.of(
        Arguments.of(0, BYTES, "0 B"),
        Arguments.of(1048576, BYTES, "1048576 B"),
        Arguments.of(1L << 59, BYTES, "576460752303423488 B"),
        Arguments.of(0, KILOBYTES, "0 KB"),
        Arguments.of(1, KILOBYTES, "1 KB"),
        Arguments.of(1024, KILOBYTES, "1024 KB"),
        Arguments.of(16384, KILOBYTES, "16384 KB"),
        Arguments.of(0, MEGABYTES, "0 MB"),
        Arguments.of(1, MEGABYTES, "1 MB"),
        Arguments.of(16, MEGABYTES, "16 MB"),
        Arguments.of(16006, MEGABYTES, "16006 MB"),
        Arguments.of(0, GIGABYTES, "0 GB"),
        Arguments.of(1, GIGABYTES, "1 GB"),
        Arguments.of(2, GIGABYTES, "2 GB"),
        Arguments.of(1L << 29, GIGABYTES, "536870912 GB"),
        Arguments.of(1L << 30, GIGABYTES, "1073741824 GB"));
  }
}
