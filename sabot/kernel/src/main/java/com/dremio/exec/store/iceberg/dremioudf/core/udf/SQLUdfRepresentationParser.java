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
package com.dremio.exec.store.iceberg.dremioudf.core.udf;

import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.util.JsonUtil;

class SQLUdfRepresentationParser {
  private static final String BODY = "body";
  private static final String DIALECT = "dialect";
  private static final String COMMENT = "comment";

  private SQLUdfRepresentationParser() {}

  static String toJson(SQLUdfRepresentation sqlUDFRepresentation) {
    return JsonUtil.generate(gen -> toJson(sqlUDFRepresentation, gen), false);
  }

  static void toJson(SQLUdfRepresentation sqlUdfRepresentation, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(
        sqlUdfRepresentation != null, "Invalid SQL UDF representation: null");
    generator.writeStartObject();
    generator.writeStringField(UdfRepresentationParser.TYPE, sqlUdfRepresentation.type());
    generator.writeStringField(BODY, sqlUdfRepresentation.body());
    generator.writeStringField(DIALECT, sqlUdfRepresentation.dialect());
    if (!StringUtils.isEmpty(sqlUdfRepresentation.comment())) {
      generator.writeStringField(COMMENT, sqlUdfRepresentation.comment());
    }
    generator.writeEndObject();
  }

  static SQLUdfRepresentation fromJson(String json) {
    return JsonUtil.parse(json, SQLUdfRepresentationParser::fromJson);
  }

  static SQLUdfRepresentation fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json != null, "Cannot parse SQL UDF representation from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse SQL UDF representation from non-object: %s", json);

    ImmutableSQLUdfRepresentation.Builder builder =
        ImmutableSQLUdfRepresentation.builder()
            .body(JsonUtil.getString(BODY, json))
            .dialect(JsonUtil.getString(DIALECT, json))
            .comment(JsonUtil.getStringOrNull(COMMENT, json));

    return builder.build();
  }
}
