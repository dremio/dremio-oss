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
package com.dremio.datastore;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.dremio.datastore.format.Format;
import java.io.IOException;

/**
 * Runs test cases in AbstractTestByteSerializerFactory through the JSON serialization code path.
 */
public class TestByteSerializerFactoryJson extends AbstractTestByteSerializerFactory {

  @Override
  protected <T> void runCircularTest(Format<T> format, T original) throws IOException {
    final Serializer<T, byte[]> serializer = getSerializer(format);

    final String outJson = serializer.toJson(original);
    final T backJson = serializer.fromJson(outJson);

    if (original instanceof byte[]) {
      assertArrayEquals((byte[]) original, (byte[]) backJson);
    } else {
      assertEquals(original, backJson);
    }
  }
}
