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
package com.dremio.dac.explore.udfs;

import java.io.OutputStream;

import javax.inject.Inject;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.arrow.vector.types.Types.MinorType;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.vector.complex.fn.JsonWriter;

import io.netty.buffer.ArrowBuf;

/**
 * UDFs to format lists
 */
public class FormatList {
  public static final String NAME = "list_to_delimited_string";

  /**
   * turns a list into a delimited string
   */
  @FunctionTemplate(name = FormatList.NAME, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class FormatListReq implements SimpleFunction {
    @Param private FieldReader input;
    @Param(constant = true) private VarCharHolder delimiter;
    @Output private VarCharHolder out;
    @Inject private ArrowBuf buffer;
    @Workspace private byte[] delimiterArray;

    @Override
    public void setup() {
      int l = delimiter.end - delimiter.start;
      delimiterArray = new byte[l];
      delimiter.buffer.getBytes(delimiter.start, delimiterArray, 0, l);
    }

    @Override
    public void eval() {
      java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();

      com.dremio.dac.explore.udfs.FormatList.formatList(input, delimiterArray, outputStream);
      byte [] bytes = outputStream.toByteArray();

      out.start = 0;
      buffer = buffer.reallocIfNeeded(bytes.length);
      out.buffer = buffer;
      out.buffer.setBytes(0, bytes);
      out.end = bytes.length;
    }
  }

  public static void formatList(FieldReader input, byte[] delimiterArray, OutputStream output) {
    final MinorType mt = input.getMinorType();
    try {
      JsonWriter jsonWriter = new JsonWriter(output, true, false);
        switch (mt) {
        case LIST:
          // this is a pseudo class, doesn't actually contain the real reader so we have to drop down.
          boolean first = true;
          while (input.next()) {
            if (first) {
              first = false;
            } else {
              output.write(delimiterArray);
            }
            FieldReader nestedReader = input.reader();
            if (nestedReader.isSet()) {
              switch (nestedReader.getMinorType()) {
              case FLOAT4:
                output.write(String.valueOf(nestedReader.readFloat()).getBytes());
                break;
              case FLOAT8:
                output.write(String.valueOf(nestedReader.readDouble()).getBytes());
                break;
              case INT:
                output.write(String.valueOf(nestedReader.readInteger()).getBytes());
                break;
              case SMALLINT:
                output.write(String.valueOf(nestedReader.readShort()).getBytes());
                break;
              case TINYINT:
                output.write(String.valueOf(nestedReader.readByte()).getBytes());
                break;
              case BIGINT:
                output.write(String.valueOf(nestedReader.readLong()).getBytes());
                break;
              case BIT:
                output.write(String.valueOf(nestedReader.readBoolean()).getBytes());
                break;
              case DATEMILLI:
              case TIMEMILLI:
              case TIMESTAMPMILLI:
                output.write(String.valueOf(nestedReader.readLocalDateTime()).getBytes());
                break;
              case INTERVALYEAR:
              case INTERVALDAY:
              case LIST:
                while (nestedReader.next()) {
                  jsonWriter.write(nestedReader);
                }
                break;
              case MAP:
                for (String name : nestedReader) {
                  jsonWriter.write(nestedReader.reader(name));
                }
                break;
              case NULL:
                break;
              case VARCHAR:
                output.write(String.valueOf(nestedReader.readText()).getBytes());
                break;
              case VARBINARY:
                output.write(nestedReader.readByteArray());
                break;
              default:
                throw new IllegalStateException(String.format("Unable to handle type %s.", nestedReader.getMinorType()));
              }
            }
          }
          break;
        default: // just one element
          jsonWriter.write(input);
          break;
        }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
