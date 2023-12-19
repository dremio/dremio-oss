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
package com.dremio.exec.store.deltalake;

import static com.dremio.exec.store.deltalake.DeltaConstants.STATS_PARSED_NUM_RECORDS;

import java.io.IOException;
import java.util.Optional;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class DeltaLogReaderUtils {
  public static boolean isFullRowcountEnabled(OptionManager options) {
    return options.getOption(ExecConstants.DELTA_LAKE_ENABLE_FULL_ROWCOUNT);
  }

  public static Optional<Long> parseStatsFromJson(String jsonString) throws IOException {
    JsonFactory jFactory = new JsonFactory();
    try (JsonParser jParser = jFactory.createParser(jsonString)) {

      while (jParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldname = jParser.getCurrentName();

        if (STATS_PARSED_NUM_RECORDS.equals(fieldname)) {
          jParser.nextToken();
          return Optional.of(jParser.getLongValue());
        }

      }
    } catch (IOException ex) {  // Catch IOException without doing anything
    }
    return Optional.empty();
  }
}
