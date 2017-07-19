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
package com.dremio.exec.expr.fn.impl;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;

import java.util.Iterator;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.vector.complex.MapUtility;
import com.google.common.base.Charsets;

import io.netty.buffer.ArrowBuf;

public class MappifyUtility {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MappifyUtility.class);

  // Default names used in the map.
  public static final String fieldKey = "key";
  public static final String fieldValue = "value";

  public static ArrowBuf mappify(FieldReader reader, BaseWriter.ComplexWriter writer, ArrowBuf buffer) {
    // Currently we expect single map as input
    if (reader.getMinorType() != getArrowMinorType(MinorType.MAP)) {
      throw UserException.functionError().message("The kvgen function can only be used when operating against maps.").build(logger);
    }
    BaseWriter.ListWriter listWriter = writer.rootAsList();
    listWriter.startList();
    BaseWriter.MapWriter mapWriter = listWriter.map();

    // Iterate over the fields in the map
    Iterator<String> fieldIterator = reader.iterator();
    while (fieldIterator.hasNext()) {
      String str = fieldIterator.next();
      FieldReader fieldReader = reader.reader(str);

      // Skip the field if its null
      if (fieldReader.isSet() == false) {
        mapWriter.end();
        continue;
      }

      // writing a new field, start a new map
      mapWriter.start();

      // write "key":"columnname" into the map
      VarCharHolder vh = new VarCharHolder();
      byte[] b = str.getBytes(Charsets.UTF_8);
      buffer = buffer.reallocIfNeeded(b.length);
      buffer.setBytes(0, b);
      vh.start = 0;
      vh.end = b.length;
      vh.buffer = buffer;
      mapWriter.varChar(fieldKey).write(vh);

      // Write the value to the map
      MapUtility.writeToMapFromReader(fieldReader, mapWriter);

      mapWriter.end();
    }
    listWriter.endList();

    return buffer;
  }
}

