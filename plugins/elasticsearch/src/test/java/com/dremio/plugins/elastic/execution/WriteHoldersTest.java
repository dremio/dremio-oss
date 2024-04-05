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
package com.dremio.plugins.elastic.execution;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test the WriteHolders subclasses */
@RunWith(MockitoJUnitRunner.class)
public class WriteHoldersTest {
  private static final double DOUBLE_VALUE = 1.36331;
  private WriteHolders.DoubleWriteHolder doubleWriteHolder;
  @Mock private Float8Writer float8Writer;
  @Mock private JsonParser jsonParser;

  @Before
  public void setup() throws Exception {
    doubleWriteHolder = new WriteHolders.DoubleWriteHolder("foo");
    when(jsonParser.getDoubleValue()).thenReturn(DOUBLE_VALUE);
    when(jsonParser.getValueAsString()).thenReturn(Double.toString(DOUBLE_VALUE));
  }

  @Test
  public void doubleFromFloatTest() throws Exception {
    doubleWriteHolder.write(float8Writer, JsonToken.VALUE_NUMBER_FLOAT, jsonParser);
    verify(float8Writer).writeFloat8(DOUBLE_VALUE);
  }

  @Test
  public void doubleFromIntTest() throws Exception {
    doubleWriteHolder.write(float8Writer, JsonToken.VALUE_NUMBER_INT, jsonParser);
    verify(float8Writer).writeFloat8(DOUBLE_VALUE);
  }

  @Test
  public void doubleFromDoubleTest() throws Exception {
    doubleWriteHolder.write(float8Writer, JsonToken.NOT_AVAILABLE, jsonParser);
    verify(float8Writer).writeFloat8(DOUBLE_VALUE);
  }
}
