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
package com.dremio.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.dac.util.InvalidQueryErrorConverter;

/**
 * InvalidQueryErrorConverter tests
 */
public class TestInvalidQueryErrorConverter {
  @Test
  public void convertNullTypeError() throws Exception {
    String actual = InvalidQueryErrorConverter.convert("Illegal use of 'NULL'");
    String expected = "We do not support NULL as a return data type in SELECT. Cast it to a different type.";
    assertEquals(expected, actual);
  }

  @Test
  public void convertUnknownError() throws Exception {
    String actual = InvalidQueryErrorConverter.convert("Unknown error message");
    String expected = "Unknown error message";
    assertEquals(expected, actual);
  }
}
