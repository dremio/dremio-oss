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
package com.dremio.common.serde;

import static org.junit.Assert.assertFalse;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import com.dremio.common.arrow.DremioArrowSchema;

/**
 * To test BackwardCompatibleSchemaDeTest
 */
public class BackwardCompatibleSchemaDeTest {

  private static final String OLD_SCHEMA =
    "{\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"R_REGIONKEY\",\n" +
      "    \"nullable\" : true,\n" +
      "    \"type\" : {\n" +
      "      \"name\" : \"int\",\n" +
      "      \"bitWidth\" : 64,\n" +
      "      \"isSigned\" : true\n" +
      "    },\n" +
      "    \"children\" : [{\n" +
      "      \"name\" : \"c\",\n" +
      "      \"nullable\" : true,\n" +
      "      \"type\" : {\n" +
      "        \"name\" : \"int\",\n" +
      "        \"bitWidth\" : 16,\n" +
      "        \"isSigned\" : true},\n" +
      "       \"typeLayout\" : {\n" +
      "      \"vectors\" : [ {\n" +
      "        \"type\" : \"VALIDITY\",\n" +
      "        \"typeBitWidth\" : 1\n" +
      "      }, {\n" +
      "        \"type\" : \"DATA\",\n" +
      "        \"typeBitWidth\" : 64\n" +
      "      } ]\n" +
      "      },\n" +
      "      \"children\" : [ ],\n" +
      "      \"metadata\" : {\n" +
      "        \"abc\" : \"bcd\",\n" +
      "        \"kef\" : \"dbg\"\n" +
      "      }\n" +
      "    }, {\n" +
      "      \"name\" : \"d\",\n" +
      "      \"nullable\" : true,\n" +
      "      \"type\" : {\n" +
      "        \"name\" : \"utf8\"\n" +
      "      },\n" +
      "      \"children\" : [ ],\n" +
      "      \"metadata\" : {\n" +
      "        \"abc\" : \"bcd\",\n" +
      "        \"kef\" : \"dbg\"\n" +
      "      }\n" +
      "    } ],\n" +
      "    \"typeLayout\" : {\n" +
      "      \"vectors\" : [ {\n" +
      "        \"type\" : \"VALIDITY\",\n" +
      "        \"typeBitWidth\" : 1\n" +
      "      }, {\n" +
      "        \"type\" : \"DATA\",\n" +
      "        \"typeBitWidth\" : 64\n" +
      "      } ]\n" +
      "    }\n" +
      "  }, {\n" +
      "    \"name\" : \"R_NAME\",\n" +
      "    \"nullable\" : true,\n" +
      "    \"type\" : {\n" +
      "      \"name\" : \"utf8\"\n" +
      "    },\n" +
      "    \"children\" : [ ],\n" +
      "    \"typeLayout\" : {\n" +
      "      \"vectors\" : [ {\n" +
      "        \"type\" : \"VALIDITY\",\n" +
      "        \"typeBitWidth\" : 1\n" +
      "      }, {\n" +
      "        \"type\" : \"OFFSET\",\n" +
      "        \"typeBitWidth\" : 32\n" +
      "      }, {\n" +
      "        \"type\" : \"DATA\",\n" +
      "        \"typeBitWidth\" : 8\n" +
      "      } ]\n" +
      "    }\n" +
      "  }, {\n" +
      "    \"name\" : \"R_COMMENT\",\n" +
      "    \"nullable\" : true,\n" +
      "    \"type\" : {\n" +
      "      \"name\" : \"utf8\"\n" +
      "    },\n" +
      "    \"children\" : [ ],\n" +
      "    \"typeLayout\" : {\n" +
      "      \"vectors\" : [ {\n" +
      "        \"type\" : \"VALIDITY\",\n" +
      "        \"typeBitWidth\" : 1\n" +
      "      }, {\n" +
      "        \"type\" : \"OFFSET\",\n" +
      "        \"typeBitWidth\" : 32\n" +
      "      }, {\n" +
      "        \"type\" : \"DATA\",\n" +
      "        \"typeBitWidth\" : 8\n" +
      "      } ]\n" +
      "    }\n" +
      "  } ]\n" +
      "}";

  @Test
  public void testBackwardCompatofSchema() throws Exception {
    Schema schema = DremioArrowSchema.fromJSON(OLD_SCHEMA);
    // should not fail with serialization exception

    String newJson = schema.toJson();

    assertFalse(newJson.contains("typeLayout"));
  }
}
