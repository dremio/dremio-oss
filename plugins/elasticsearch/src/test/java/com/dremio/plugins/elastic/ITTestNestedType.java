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
package com.dremio.plugins.elastic;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for the nested data type. */
public class ITTestNestedType extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestNestedType.class);

  private static final String NESTED_TYPE_MAPPING = "/json/nested-type/nested-type-mapping.json";
  private static final String NESTED_TYPE_DATA = "/json/nested-type/data-1/";
  private static final String NESTED_TYPE_DATA_1 =
      "/json/nested-type/data-1/nested-type-data-1.json";
  private static final String NESTED_TYPE_DATA_2 =
      "/json/nested-type/data-1/nested-type-data-2.json";

  private static final DateTimeFormatter EXPECTED_RESULTS_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSZ");

  @Test
  public void testNestedType_SelectNamedTopLevel() throws Exception {

    elastic.load(schema, table, NESTED_TYPE_MAPPING, NESTED_TYPE_DATA_1);

    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));

    String sql =
        " select id, username, address "
            + " from "
            + " elasticsearch."
            + schema
            + "."
            + table
            + " t";

    testBuilder()
        .sqlQuery(sql)
        .baselineColumns("id", "username", "address")
        .unOrdered()
        .baselineValues(
            1,
            "andrew",
            mapOf(
                "zip",
                94026,
                "city",
                "menlo park",
                "street",
                "1 santa cruz",
                "phones",
                mapOf(
                    "mobile", 4152568900L,
                    "office", 4156263434L),
                "state",
                "ca",
                "creationDate",
                parseLocalDateTime("20180112T072408.617+0000")))
        .go();
  }

  @Test
  @Ignore("DX-4902")
  public void testNestedType_SelectNamedSecondLevel() throws Exception {

    elastic.load(schema, table, NESTED_TYPE_MAPPING, NESTED_TYPE_DATA_1);

    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));

    String sql =
        " select t.address.city as city, t.address.state as state, t.address.phones as phones "
            + " from "
            + " elasticsearch."
            + schema
            + "."
            + table
            + " t";

    // TODO DX-4902 - this schema returned when this is pushed down is currently incorrect,
    // The test above that runs a select *, rather than referencing the fields individually returns
    // the correct schema
    // with the phone numbers as long type.
    // For now this is avoiding running the second phase of the test with project pushdown
    // disabled. To re-enable, just use the testBuilder() helper method rather than calling this
    // constructor directly
    testBuilder()
        .sqlQuery(sql)
        .baselineColumns("city", "state", "phones")
        .unOrdered()
        .baselineValues(
            "menlo park",
            "ca",
            mapOf(
                "mobile", 4152568900D,
                "office", 4156263434D))
        .go();
  }

  @Test
  public void testNestedType_SelectNamedArrayOfNested() throws Exception {

    elastic.load(schema, table, NESTED_TYPE_MAPPING, NESTED_TYPE_DATA_2);

    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));
    logger.info("--> search:\n{}", elastic.search(schema, table));

    String sql =
        " select id, username, address "
            + " from "
            + " elasticsearch."
            + schema
            + "."
            + table
            + " t";

    testBuilder()
        .sqlQuery(sql)
        .baselineColumns("id", "username", "address")
        .unOrdered()
        .baselineValues(
            2,
            "alexa",
            listOf(
                mapOf(
                    "zip",
                    94301,
                    "city",
                    "palo alto",
                    "street",
                    "1 hamilton st.",
                    "phones",
                    mapOf(
                        "mobile", 6504501234L,
                        "office", 6504809876L),
                    "state",
                    "ca",
                    "creationDate",
                    parseLocalDateTime("20161112T025252.139+0000")),
                mapOf(
                    "zip",
                    94040,
                    "city",
                    "mountain view",
                    "street",
                    "1 california ave.",
                    "phones",
                    mapOf(
                        "mobile", 6504443212L,
                        "office", 4083456789L),
                    "state",
                    "ca",
                    "creationDate",
                    parseLocalDateTime("20200112T022252.149+0000"))))
        .go();
  }

  @Test
  @Ignore("DX-4902")
  public void testNestedType_SelectNamedTopLevelArray() throws Exception {

    elastic.load(schema, table, NESTED_TYPE_MAPPING, NESTED_TYPE_DATA_2);

    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));
    logger.info("--> search:\n{}", elastic.search(schema, table));

    String sql =
        " select id, username, "
            + " t.address[0].city as city_0, "
            + " t.address[1].city as city_1, "
            + " t.address[0].phones as phones_0, "
            + " t.address[1].phones.office as phones_1_office "
            + " from "
            + " elasticsearch."
            + schema
            + "."
            + table
            + " t";

    // TODO DX-4902 - this schema returned when this is pushed down is currently incorrect,
    // The test above that runs a select *, rather than referencing the fields individually returns
    // the correct schema
    // with the phone numbers as long type.
    // For now this is avoiding running the second phase of the test with project pushdown
    // disabled. To re-enable, just use the testBuilder() helper method rather than calling this
    // constructor directly
    testBuilder()
        .sqlQuery(sql)
        .baselineColumns("id", "username", "city_0", "city_1", "phones_0", "phones_1_office")
        .unOrdered()
        .baselineValues(
            2,
            "alexa",
            "palo alto",
            "mountain view",
            mapOf(
                "mobile", 6504501234D,
                "office", 6504809876D),
            4083456789L)
        .go();
  }

  @org.junit.Ignore("flapping test on ci machine")
  @Test
  public void testNestedType_SelectNamedMixedScalarAndArray() throws Exception {

    elastic.load(schema, table, NESTED_TYPE_MAPPING, NESTED_TYPE_DATA);

    logger.info("--> mapping:\n{}", elastic.mapping(schema, table));
    logger.info("--> search:\n{}", elastic.search(schema, table));

    String sql =
        " select id, username, t.address"
            + " from "
            + " elasticsearch."
            + schema
            + "."
            + table
            + " t"
            + " order by id ";

    testBuilder()
        .sqlQuery(sql)
        .baselineColumns("id", "username", "address")
        .ordered()
        .baselineValues(
            1,
            "andrew",
            mapOf(
                "city", "menlo park",
                "state", "ca",
                "zip", 94026,
                "street", "1 santa cruz",
                "phones",
                    mapOf(
                        "mobile", 4152568900L,
                        "office", 4156263434L)))
        .baselineValues(
            2,
            "alexa",
            listOf(
                mapOf(
                    "city", "palo alto",
                    "state", "ca",
                    "zip", 94301,
                    "street", "1 hamilton st.",
                    "phones",
                        mapOf(
                            "mobile", 6504501234L,
                            "office", 6504809876L)),
                mapOf(
                    "city", "mountain view",
                    "state", "ca",
                    "zip", 94040,
                    "street", "1 california ave.",
                    "phones",
                        mapOf(
                            "mobile", 6504443212L,
                            "office", 4083456789L))))
        .go();
  }

  private static LocalDateTime parseLocalDateTime(String value) {
    return LocalDateTime.parse(value, EXPECTED_RESULTS_FORMATTER);
  }
}
