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

import com.dremio.common.expression.SchemaPath;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.calcite.util.Pair;

/** Elasticsearch Json Reader for ES7 Version. Overrides {@link ElasticsearchJsonReader} */
public class ElasticSearch7JsonReader extends ElasticsearchJsonReader {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ElasticSearch7JsonReader.class);

  public ElasticSearch7JsonReader(
      ArrowBuf managedBuf,
      List<SchemaPath> columns,
      String resourceName,
      FieldReadDefinition rootDefinition,
      boolean fieldsProjected,
      boolean metaUIDSelected,
      boolean metaIDSelected,
      boolean metaTypeSelected,
      boolean metaIndexSelected) {
    super(
        managedBuf,
        columns,
        resourceName,
        rootDefinition,
        fieldsProjected,
        metaUIDSelected,
        metaIDSelected,
        metaTypeSelected,
        metaIndexSelected);
  }

  @Override
  public Pair<String, Long> getScrollAndTotalSizeThenSeekToHits() throws IOException {
    final JsonToken token = seekForward(ElasticsearchConstants.SCROLL_ID);
    Preconditions.checkState(token == JsonToken.VALUE_STRING, "Invalid response");

    String scrollId = parser.getValueAsString();

    seekForward(ElasticsearchConstants.HITS);
    seekForward(ElasticsearchConstants.TOTAL_HITS);
    final JsonToken totalSizeToken = seekForward(ElasticsearchConstants.TOTAL_HITS_VALUE);
    Preconditions.checkState(totalSizeToken == JsonToken.VALUE_NUMBER_INT, "Invalid response");
    final long totalSize = parser.getValueAsLong();

    seekForward(ElasticsearchConstants.RELATION);
    parser.getValueAsString();
    parser.nextToken();

    final JsonToken hitsToken = seekForward(ElasticsearchConstants.HITS);
    Preconditions.checkState(hitsToken == JsonToken.START_ARRAY, "Invalid response");
    return new Pair<>(scrollId, totalSize);
  }

  /**
   * Seek to desired field name.
   *
   * @param fieldName The name of the field to find.
   * @return Either the current token or null if the position wasn't found.
   * @throws IOException
   * @throws JsonParseException
   */
  private JsonToken seekForward(String fieldName) throws IOException, JsonParseException {
    JsonToken token = null;

    String currentName;
    token = parser.getCurrentToken();
    if (token == null) {
      token = parser.nextToken();
    }

    while ((token = parser.nextToken()) != null) {
      if (token == JsonToken.START_OBJECT) {
        token = parser.nextToken();
      }
      if (token == JsonToken.FIELD_NAME) {
        // found a node, go one level deep
        currentName = parser.getCurrentName();
        if (currentName.equals(fieldName)) {
          return parser.nextToken();
        } else {
          // get field token (can be value, object or array)
          parser.nextToken();
          parser.skipChildren();
        }
      } else {
        break;
      }
    }

    return null;
  }
}
