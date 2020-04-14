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
package com.dremio.exec.physical.impl;

import java.math.BigDecimal;

import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestJoinJson6ByteCopier extends BaseTestQuery {
    @Test
    public void testJoin() throws Exception {
        /*
         * Panoramic test
         * Test for checking that bugfix in ConditionalFieldBufferCopier6#copy function is working correctly.
         * I was only able to simulate it with parquet files, not with csv files.
         */
        String q = "SELECT * FROM cp.\"join/json_copier_6_byte/dremio_join_entity/1_0_0.parquet\" t1 " +
                "left join\n" +
                "cp.\"join/json_copier_6_byte/dremio_join_tags/1_0_0.parquet\" t2\n" +
                "ON\n" +
                "t1.id = t2.entity_id";
        test(q);

        JsonStringHashMap<String, Long> expected_json_struct = new JsonStringHashMap<>();
        expected_json_struct.put("id", 3L);

        testBuilder()
                .sqlQuery(q)
                .ordered()
                .baselineColumns("ID", "entity_id", "tags_struct", "tags")
                .baselineValues(BigDecimal.valueOf(1), null, null, null)
                .baselineValues(BigDecimal.valueOf(2), null, null, null)
                .baselineValues(
                        BigDecimal.valueOf(3),
                        BigDecimal.valueOf(3),
                        expected_json_struct,
                        "{\n  \"id\": 3\n}")
                .build()
                .run();

    }
}
