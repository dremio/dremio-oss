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
package com.dremio.exec.store.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.exec.store.iceberg.viewdepoc.BaseVersion;
import com.dremio.exec.store.iceberg.viewdepoc.DremioViewVersionMetadataParser;
import com.dremio.exec.store.iceberg.viewdepoc.Version;
import com.dremio.exec.store.iceberg.viewdepoc.VersionSummary;
import com.dremio.exec.store.iceberg.viewdepoc.ViewDefinition;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadata;
import com.dremio.exec.store.iceberg.viewdepoc.ViewVersionMetadataParser;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.Test;

/** This test should be deleted when OSS Iceberg view is ready */
public class TestDremioViewVersionMetadataParser {

  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));
  private static final String location = "s3://bucket/test/location";
  private static final Map<String, String> properties = ImmutableMap.of("some-key", "some-value");
  private static final ViewDefinition viewDefinition =
      ViewDefinition.of(
          "select * from mySource.myTable",
          TEST_SCHEMA,
          "session-catalog",
          Collections.singletonList("session-namespace"));
  private static final VersionSummary versionSummary = new VersionSummary(properties);
  private static final BaseVersion version1 =
      new BaseVersion(1, 1, 1, versionSummary, viewDefinition);

  @Test
  public void testFromJson() {
    ViewVersionMetadata expected =
        ViewVersionMetadata.newViewVersionMetadata(version1, location, viewDefinition, properties);
    String json = JsonUtil.generate(gen -> ViewVersionMetadataParser.toJson(expected, gen), true);

    ViewVersionMetadata actual = DremioViewVersionMetadataParser.fromJson(json);
    assertThat(actual)
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Schema.class)
        // We have to ignore BaseVersion here. In Iceberg's VersionParser.class, it just overwrites
        // whatever we have with three properties:
        // operation, genie_id, engine_version. Other parts of the baseVersion is compared later
        // loop.
        .ignoringFieldsOfTypes(BaseVersion.class)
        .isEqualTo(expected);

    for (int i = 0; i < actual.versions().size(); i++) {
      Version actualVersion = actual.version(i);
      Version expectedVersion = expected.version(i);
      assertThat(actualVersion)
          .usingRecursiveComparison()
          .ignoringFields("summary.properties")
          .isEqualTo(expectedVersion);
    }

    assertThat(expected.definition().schema().sameSchema(actual.definition().schema())).isTrue();
  }

  @Test
  public void testNullCheck() {
    assertThatThrownBy(() -> DremioViewVersionMetadataParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse view metadata from null string");
  }

  @Test
  public void testInvalidFormatVersion() {
    assertThatThrownBy(
            () ->
                DremioViewVersionMetadataParser.fromJson(
                    DremioViewVersionMetadataParserHelper.jsonInvalidFormatVersion))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot read unsupported version");
  }

  @Test
  public void testMissingLocation() {
    assertThatThrownBy(
            () ->
                DremioViewVersionMetadataParser.fromJson(
                    DremioViewVersionMetadataParserHelper.jsonMissingLocation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: location");
  }

  @Test
  public void testMissionCurrentVersionId() throws Exception {
    assertThatThrownBy(
            () ->
                DremioViewVersionMetadataParser.fromJson(
                    DremioViewVersionMetadataParserHelper.jsonMissingCurrentVersionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: current-version-id");
  }
}
