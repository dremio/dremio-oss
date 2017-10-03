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
package com.dremio.exec.store.easy.json.reader;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;
import org.apache.arrow.vector.complex.writer.BaseWriter;

/**
 * JSON parser that just parses records and doesn't write to output.
 * This reader can unwrap a single root array record and treat it like a set of distinct records.
 */
public class CountingJsonReader extends BaseJsonProcessor {

  private boolean inOuterList = false;

  @Override
  public ReadState write(BaseWriter.ComplexWriter writer) throws IOException {
    JsonToken token = parser.nextToken();
    if (!parser.hasCurrentToken()) {
      return ReadState.END_OF_STREAM;
    }

    switch (token) {
      case START_ARRAY:
        if (inOuterList) {
          throwIllegalStartException();
        }
        token = parser.nextToken();
        if (token == JsonToken.START_OBJECT) {
          inOuterList = true;
        } else {
          throwIllegalStartException();
        }
        break;
      case START_OBJECT:
        break;
      case END_ARRAY:
        if (inOuterList) {
          return ReadState.END_OF_STREAM;
        }
        throwIllegalStartException();
      default:
        throwIllegalStartException();
    }

    parser.skipChildren();
    return ReadState.WRITE_SUCCEED;
  }

  private void throwIllegalStartException() {
    throw new IllegalStateException(
        "The top level of your document must either be a single array of maps or a set of white space delimited maps.");
  }

  @Override
  public void ensureAtLeastOneField(BaseWriter.ComplexWriter writer) {

  }
}
