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
package com.dremio.exec.expr.fn.impl.array;

import com.dremio.exec.expr.fn.FunctionErrorContext;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;

public class ArrayToStringHelper {
  private static final String NULL = "NULL";

  public static byte[] arrayToDelimitedStringByteArray(
      FieldReader input, byte[] delimiterArray, FunctionErrorContext errCtx) {

    java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();
    try {
      toDelimitedString(input, delimiterArray, outputStream);
    } catch (java.io.IOException e) {
      throw errCtx.error().message("%s", e).build();
    }
    return outputStream.toByteArray();
  }

  private static void toDelimitedString(
      FieldReader input, byte[] delimiterArray, OutputStream output) throws IOException {
    if (input.getMinorType().equals(Types.MinorType.LIST)) {
      boolean first = true;
      while (input.next()) {
        if (first) {
          first = false;
        } else {
          output.write(delimiterArray);
        }
        writeValue(output, input.reader());
      }
    } else {
      writeValue(output, input);
    }
  }

  private static void writeValue(OutputStream output, FieldReader input) throws IOException {
    if (input.readObject() == null) {
      return;
    }
    switch (input.getMinorType()) {
      case INTERVALYEAR:
      case INTERVALDAY:
      case LIST:
        output.write("[".getBytes());
        while (input.next()) {
          writeValue(output, input.reader());
          if (input.size() > 0) {
            output.write(",".getBytes());
          }
        }
        output.write("]".getBytes());
        break;
      case STRUCT:
        output.write("{".getBytes());
        Iterator<String> fieldNames = input.iterator();
        while (fieldNames.hasNext()) {
          String name = fieldNames.next();
          FieldReader childReader = input.reader(name);
          output.write(name.getBytes());
          output.write(":".getBytes());
          writeValue(output, childReader);
          if (fieldNames.hasNext()) {
            output.write(",".getBytes());
          }
        }
        output.write("}".getBytes());
        break;
      case DATEMILLI:
        output.write(String.valueOf(input.readLocalDateTime().toLocalDate()).getBytes());
        break;
      default:
        output.write(String.valueOf(input.readObject()).getBytes());
        break;
    }
  }
}
