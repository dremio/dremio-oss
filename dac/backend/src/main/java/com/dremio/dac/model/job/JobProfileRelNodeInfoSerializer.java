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
package com.dremio.dac.model.job;

import java.io.IOException;

import com.dremio.exec.proto.UserBitShared;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.util.JsonFormat;

public class JobProfileRelNodeInfoSerializer extends JsonSerializer<UserBitShared.RelNodeInfo> {

  private static final JsonFormat.Printer PROTOBUF_JSON_PRINTER = JsonFormat.printer();

  @Override
  public void serialize(UserBitShared.RelNodeInfo relNodeInfo, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
    throws IOException {
    jsonGenerator.writeRawValue(PROTOBUF_JSON_PRINTER.print(relNodeInfo));
  }
}
