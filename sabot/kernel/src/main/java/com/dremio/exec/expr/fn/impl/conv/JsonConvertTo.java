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

package com.dremio.exec.expr.fn.impl.conv;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.VarBinaryHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;

/**
 * The two functions defined here convert_toJSON and convert_toEXTENDEDJSON are almost
 * identical. For now, the default behavior is to use simple JSON (see DRILL-2976). Until the issues with
 * extended JSON types are resolved, the convert_toJSON/convert_toSIMPLEJSON is consider the default. The default
 * will possibly change in the future, as the extended types can accurately serialize more types supported
 * by Dremio.
 * TODO(DRILL-2906) - review the default once issues with extended JSON are resolved
 */
public class JsonConvertTo {

 static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonConvertTo.class);

  private JsonConvertTo(){}

  @FunctionTemplate(names = { "convert_toJSON", "convert_toSIMPLEJSON" } , scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ConvertToJson implements SimpleFunction{

    @Param FieldReader input;
    @Output VarBinaryHolder out;
    @Inject ArrowBuf buffer;
    @Inject FunctionErrorContext errCtx;

    @Override
    public void setup(){
    }

    @Override
    public void eval(){
      if (input.isSet()) {
        out.start = 0;

        java.io.ByteArrayOutputStream stream = new java.io.ByteArrayOutputStream();
        try {
          com.dremio.exec.vector.complex.fn.JsonWriter jsonWriter = new com.dremio.exec.vector.complex.fn.JsonWriter(stream, true, false);

          jsonWriter.write(input);
        } catch (Exception e) {
          throw errCtx.error()
            .message("%s", e)
            .build();
        }

        byte[] bytea = stream.toByteArray();
        buffer = buffer.reallocIfNeeded(bytea.length);
        out.buffer = buffer;
        out.buffer.setBytes(0, bytea);
        out.end = bytea.length;
      }
    }
  }

  @FunctionTemplate(name = "convert_toEXTENDEDJSON", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ConvertToExtendedJson implements SimpleFunction{

    @Param FieldReader input;
    @Output VarBinaryHolder out;
    @Inject ArrowBuf buffer;
    @Inject FunctionErrorContext errCtx;

    @Override
    public void setup(){
    }

    @Override
    public void eval(){
      out.start = 0;

      java.io.ByteArrayOutputStream stream = new java.io.ByteArrayOutputStream();
      try {
        com.dremio.exec.vector.complex.fn.JsonWriter jsonWriter = new com.dremio.exec.vector.complex.fn.JsonWriter(stream, true, true);

        jsonWriter.write(input);
      } catch (Exception e) {
        throw errCtx.error()
          .message("%s", e)
          .build();
      }

      byte [] bytea = stream.toByteArray();
      buffer = buffer.reallocIfNeeded(bytea.length);
      out.buffer = buffer;
      out.buffer.setBytes(0, bytea);
      out.end = bytea.length;
    }
  }
}
