/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.vector.complex.fn;

import static com.dremio.common.util.MajorTypeHelper.getMinorTypeFromArrowMinorType;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.arrow.vector.complex.reader.FieldReader;

import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MinorType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;

public class JsonWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonWriter.class);

  private final JsonFactory factory = new JsonFactory();
  private final JsonOutput gen;

  public JsonWriter(OutputStream out, boolean pretty, boolean useExtendedOutput) throws IOException{
    JsonGenerator writer = factory.createJsonGenerator(out);
    if(pretty){
      writer = writer.useDefaultPrettyPrinter();
    }
    if(useExtendedOutput){
      gen = new ExtendedJsonOutput(writer);
    }else{
      gen = new BasicJsonOutput(writer);
    }

  }

  public JsonWriter(JsonOutput gen) {
    this.gen = gen;
  }

  public void write(FieldReader reader) throws JsonGenerationException, IOException{
    writeValue(reader);
    gen.flush();
  }

  private void writeValue(FieldReader reader) throws JsonGenerationException, IOException{
    final DataMode m = DataMode.OPTIONAL;
    final MinorType mt = getMinorTypeFromArrowMinorType(reader.getMinorType());

    switch(m){
    case OPTIONAL:
    case REQUIRED:


      switch (mt) {
      case FLOAT4:
        gen.writeFloat(reader);
        break;
      case FLOAT8:
        gen.writeDouble(reader);
        break;
      case INT:
        gen.writeInt(reader);
        break;
      case SMALLINT:
        gen.writeSmallInt(reader);
        break;
      case TINYINT:
        gen.writeTinyInt(reader);
        break;
      case BIGINT:
        gen.writeBigInt(reader);
        break;
      case BIT:
        gen.writeBoolean(reader);
        break;

      case DATE:
        gen.writeDate(reader);
        break;
      case TIME:
        gen.writeTime(reader);
        break;
      case TIMESTAMP:
        gen.writeTimestamp(reader);
        break;
      case INTERVALYEAR:
      case INTERVALDAY:
      case INTERVAL:
        gen.writeInterval(reader);
        break;
      case DECIMAL28DENSE:
      case DECIMAL28SPARSE:
      case DECIMAL38DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL9:
      case DECIMAL18:
        gen.writeDecimal(reader);
        break;

      case LIST:
        // this is a pseudo class, doesn't actually contain the real reader so we have to drop down.
        gen.writeStartArray();
        while (reader.next()) {
          writeValue(reader.reader());
        }
        gen.writeEndArray();
        break;
      case MAP:
        gen.writeStartObject();
        if (reader.isSet()) {
          for(String name : reader){
            FieldReader childReader = reader.reader(name);
            if(childReader.isSet()){
              gen.writeFieldName(name);
              writeValue(childReader);
            }
          }
        }
        gen.writeEndObject();
        break;
      case NULL:
      case LATE:
        gen.writeUntypedNull();
        break;

      case VAR16CHAR:
        gen.writeVar16Char(reader);
        break;
      case VARBINARY:
        gen.writeBinary(reader);
        break;
      case VARCHAR:
        gen.writeVarChar(reader);
        break;

      }
      break;
    }

  }

}
