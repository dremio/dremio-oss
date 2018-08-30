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
package com.dremio.plugins.elastic;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tests for the object data type.
 */
public class ITTestObjectType extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestObjectType.class);

  private static final String EXPLICIT_JSON_MAPPING = "/json/object-type/explicit-object-type.json";
  private static final String IMPLICIT_JSON_MAPPING = "/json/object-type/implicit-object-type.json";
  private static final String JSON_DATA = "/json/object-type/data-1/";

  @Test
  public void testExplicitObjectType() throws Exception {

    elastic.load(schema, table, EXPLICIT_JSON_MAPPING, JSON_DATA);


    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));
    logger.info("--> index contents:\n{}", elastic.search(schema, table));

    String sql =
            " select t.message as message, t.person.sid as sid, " +
                    " t.person.name.first_name as first_name, t.person.name.last_name as last_name " +
                    " from " +
                    " elasticsearch." + schema + "." + table + " t";

    testBuilder()
            .sqlQuery(sql)
            .baselineColumns("message", "sid", "first_name", "last_name")
            .unOrdered()
            .baselineValues("This is a message.", 12345, "BillyBob", "Hecka")
            .baselineValues("This is a funny tweet!", 12346, "Andrew", "S")
            .go();
  }

  @Test
  public void testImplicitObjectType() throws Exception {

    elastic.load(schema, table, IMPLICIT_JSON_MAPPING, JSON_DATA);

    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));
    logger.info("--> index contents:\n{}", elastic.search(schema, table));

    String sql =
            " select t.message as message, t.person.sid as sid, " +
                    " t.person.name.first_name as first_name, t.person.name.last_name as last_name " +
                    " from " +
                    " elasticsearch." + schema + "." + table + " t";

    testBuilder()
            .sqlQuery(sql)
            .baselineColumns("message", "sid", "first_name", "last_name")
            .unOrdered()
            .baselineValues("This is a message.", 12345, "BillyBob", "Hecka")
            .baselineValues("This is a funny tweet!", 12346, "Andrew", "S")
            .go();
  }
}
