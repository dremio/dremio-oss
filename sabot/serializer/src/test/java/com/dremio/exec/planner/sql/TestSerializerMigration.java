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
package com.dremio.exec.planner.sql;

import static com.dremio.exec.planner.sql.TestSerializerRoundtrip.CATALOG;
import static com.dremio.exec.planner.sql.TestSerializerRoundtrip.CATALOG_READER;
import static com.dremio.exec.planner.sql.TestSerializerRoundtrip.FACTORY;
import static com.dremio.exec.planner.sql.TestSerializerRoundtrip.OPERATOR_TABLE;

import com.dremio.exec.planner.sql.TestSerializerRoundtrip.Output;
import com.dremio.test.GoldenFileTestBuilder.InputAndOutput;
import com.dremio.test.GoldenFileTestBuilder.MultiLineString;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

/**
 * Ensures that the current serializer can deserialize the binaries created by old serializers. This
 * is important for the migration / upgrade story: Suppose we persist a query plan and upgrade the
 * serializers, then we need to ensure the current serializer can deserialize it otherwise we will
 * end up with an exception in production.
 *
 * <p>When a developer introduces a breaking change in the proto serializer they should: 1) leave
 * the original field and rename it to "legacyXXX" 2) make a copy of the baselines and add it to the
 * migrations folder 3) Update MIGRATION_BASELINE_PATHS 4) Add the new field 5) Update the baseline
 * files
 */
public final class TestSerializerMigration {
  // Migration Paths
  private static final Path MIGRATIONS_DIRECTORY =
      Paths.get(Resources.getResource("migrations").getPath());
  private static final Path VERSION_1_19 = MIGRATIONS_DIRECTORY.resolve("1.19");
  private static final Path VERSION_24_2 = MIGRATIONS_DIRECTORY.resolve("24.2");

  private static final Path[] MIGRATION_BASELINE_PATHS = {
    VERSION_1_19.resolve("TestSerializerRoundtrip.testQueries.yaml"),
    VERSION_1_19.resolve("TestSerializerRoundtrip.testSqlFunction.yaml"),
    VERSION_1_19.resolve("TestSerializerRoundtrip.testSqlToRelConvertTests.yaml"),
    VERSION_1_19.resolve("TestSerializerRoundtrip.testTpchQueries.yaml"),
    VERSION_24_2.resolve("TestSerializerRoundtrip.testColumnAliases.yaml"),
    VERSION_24_2.resolve("TestSerializerRoundtrip.testQualify.yaml"),
    VERSION_24_2.resolve("TestSerializerRoundtrip.testQueries.yaml"),
    VERSION_24_2.resolve("TestSerializerRoundtrip.testSqlFunction.yaml"),
    VERSION_24_2.resolve("TestSerializerRoundtrip.testSqlToRelConvertTests.yaml"),
    VERSION_24_2.resolve("TestSerializerRoundtrip.testTpchQueries.yaml"),
  };

  // Deserializer
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  @Test
  public void testMigration() throws IOException {
    for (Path path : MIGRATION_BASELINE_PATHS) {
      OBJECT_MAPPER
          .readValue(
              new File(path.toUri().getPath()),
              new TypeReference<List<InputAndOutput<MultiLineString, Output>>>() {})
          .stream()
          .filter(rec -> rec.exceptionMessage == null)
          .forEach(
              rec -> {
                MockDremioQueryParser parseTool =
                    new MockDremioQueryParser(OPERATOR_TABLE, CATALOG, "user1");
                parseTool.parse(rec.input.toString());
                RelNode deserializedRelNode =
                    FACTORY
                        .getDeserializer(
                            parseTool.getCluster(), CATALOG_READER, OPERATOR_TABLE, null)
                        .deserialize(rec.output.getQueryPlanBinary().getBytes());

                String errorMessage =
                    String.format(
                        "Could not migrate '%s' query: '%s'.", rec.description, rec.input);
                MultiLineString actualQueryPlan =
                    MultiLineString.create(RelOptUtil.toString(deserializedRelNode));
                MultiLineString expectedQueryPlan = rec.output.getQueryPlanText();
                Assert.assertEquals(errorMessage, expectedQueryPlan, actualQueryPlan);
              });
    }
  }
}
