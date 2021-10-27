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
package com.dremio.dac.obfuscate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.exec.proto.beans.AccelerationProfile;
import com.dremio.exec.proto.beans.DatasetProfile;
import com.dremio.exec.proto.beans.LayoutMaterializedViewProfile;
import com.dremio.exec.proto.beans.MeasureColumn;
import com.dremio.exec.proto.beans.QueryProfile;
import com.dremio.exec.proto.beans.SubstitutionProfile;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.accelerator.AccelerationDetailsUtils;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.DatasetDetails;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import io.protostuff.ByteString;
import io.protostuff.JsonIOUtils;
import io.protostuff.Schema;

/**
 * Tool to redact a single profile
 * This is package-protected and all usages should be via ObfuscationUtils because this does not
 * check whether to obfuscate or not.
 */
class ProfileCleanser {

  private final InputStream input;
  private final OutputStream output;

  public ProfileCleanser(InputStream input, OutputStream output) {
    this.input = input;
    this.output = output;
  }

  private byte[] readAll(InputStream is) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    while(is.available() > 0) {
      os.write(is.read());
    }
    return os.toByteArray();
  }

  public void cleanse() throws IOException {
    byte[] b = readAll(input);
    QueryProfile profile = new QueryProfile();
    fromJSON(b, profile, QueryProfile.getSchema());

    cleanseQueryProfileInPlace(profile);

    toJSON(output, profile, QueryProfile.getSchema());
    output.flush();
  }

  public static void cleanseQueryProfileInPlace(QueryProfile profile) {
    String sql = profile.getQuery();
    if (sql.startsWith("[Prepare Statement] ")) {
      sql = sql.replace("[Prepare Statement] ", "");
      profile.setQuery("[Prepare Statement] " + SqlCleanser.cleanseSql(sql));
    } else {
      profile.setQuery(SqlCleanser.cleanseSql(sql));
    }

    if (!nullOrEmpty(profile.getPlan())) {
      profile.setPlan(RelCleanser.redactRelTree(profile.getPlan()));
    }
    if (!nullOrEmpty(profile.getJsonPlan())) {
      profile.setJsonPlan(ObfuscationUtils.obfuscate(profile.getJsonPlan()));
    }

    if (profile.getPlanPhasesList() != null) {
      profile.getPlanPhasesList().forEach(p -> {
        if (!nullOrEmpty(p.getPlan())) {
          p.setPlan(RelCleanser.redactRelTree(p.getPlan()));
        }
        if (!nullOrEmpty(p.getPlannerDump())) {
          p.setPlannerDump(ObfuscationUtils.obfuscate(p.getPlannerDump()));
        }
      });
    }

    if (profile.getAccelerationProfile() != null) {
      visit(profile.getAccelerationProfile());
    }

    if (profile.getDatasetProfileList() != null) {
      profile.setDatasetProfileList(profile.getDatasetProfileList().stream().map(ProfileCleanser::visitDatasetProfile).collect(Collectors.toList()));
    }
    profile.setFullSchema(null);
  }

  private static DatasetProfile visitDatasetProfile(DatasetProfile datasetProfile) {
    if (datasetProfile.getDatasetPath() != null) {
      datasetProfile.setDatasetPath(ObfuscationUtils.obfuscate(datasetProfile.getDatasetPath()));
    }
    if (datasetProfile.getSql() != null) {
      datasetProfile.setSql(SqlCleanser.cleanseSql(datasetProfile.getSql()));
    }
    if (datasetProfile.getBatchSchema() != null) {
      datasetProfile.setBatchSchema(visitBatchSchema(datasetProfile.getBatchSchema()));
    }
    return datasetProfile;
  }

  private static ByteString visitBatchSchema(ByteString schemaString) {
    BatchSchema schema = BatchSchema.deserialize(schemaString.toByteArray());
    List<Field> newFields = schema.getFields().stream()
      .map(f -> new Field(RelCleanser.redactField(f.getName()), f.getFieldType(), f.getChildren())).collect(Collectors.toList());
    return ByteString.copyFrom(new BatchSchema(newFields).serialize());
  }

  private static void visit(AccelerationProfile p) {
    if (p.getLayoutProfilesList() != null) {
      p.getLayoutProfilesList().forEach(ProfileCleanser::visit);
    }
    if (p.getNormalizedQueryPlansList() != null) {
      List<String> redacted = p.getNormalizedQueryPlansList().stream().map(RelCleanser::redactRelTree).collect(Collectors.toList());
      p.setNormalizedQueryPlansList(redacted);
    }
    if (p.getAccelerationDetails() != null) {
      p.setAccelerationDetails(visitAccelerationDetails(p.getAccelerationDetails()));
    }
  }

  private static ByteString visitAccelerationDetails(ByteString details) {
    AccelerationDetails accelDetails = AccelerationDetailsUtils.deserialize(details);
    if (accelDetails.getReflectionRelationshipsList() != null) {
      accelDetails.setReflectionRelationshipsList(accelDetails.getReflectionRelationshipsList()
        .stream().map(ProfileCleanser::visitReflectionRelationship).collect(Collectors.toList()));
    }
    return ByteString.copyFrom(AccelerationDetailsUtils.serialize(accelDetails));
  }

  private static ReflectionRelationship visitReflectionRelationship(ReflectionRelationship relationship) {
    LayoutDetailsDescriptor layout = relationship.getReflection().getDetails();
    if (layout.getDisplayFieldList() != null) {
      layout.setDisplayFieldList(layout.getDisplayFieldList().stream().peek(d -> d.setName(RelCleanser.redactField(d.getName()))).collect(Collectors.toList()));
    }
    if (layout.getDimensionFieldList() != null) {
      layout.setDimensionFieldList(layout.getDimensionFieldList().stream().peek(d -> d.setName(RelCleanser.redactField(d.getName()))).collect(Collectors.toList()));
    }
    if (layout.getMeasureFieldList() != null) {
      layout.setMeasureFieldList(layout.getMeasureFieldList().stream().peek(m -> m.setName(RelCleanser.redactField(m.getName()))).collect(Collectors.toList()));
    }
    if (layout.getPartitionFieldList() != null) {
      layout.setPartitionFieldList(layout.getPartitionFieldList().stream().peek(m -> m.setName(RelCleanser.redactField(m.getName()))).collect(Collectors.toList()));
    }
    if (layout.getSortFieldList() != null) {
      layout.setSortFieldList(layout.getSortFieldList().stream().peek(m -> m.setName(RelCleanser.redactField(m.getName()))).collect(Collectors.toList()));
    }
    if (relationship.getDataset() != null) {
      relationship.setDataset(visitDataset(relationship.getDataset()));
    }
    relationship.getReflection().setDetails(layout);
    return relationship;
  }

  private static DatasetDetails visitDataset(DatasetDetails datasetProfile) {
    List<String> redactedElements = ObfuscationUtils.obfuscate(datasetProfile.getPathList(), ObfuscationUtils::obfuscate);
    return datasetProfile.setPathList(redactedElements);
  }

  private static void visit(LayoutMaterializedViewProfile l) {
    if (l.getDimensionsList() != null) {
      l.setDimensionsList(l.getDimensionsList().stream().map(RelCleanser::redactField).collect(Collectors.toList()));
    }
    if (l.getMeasureColumnsList() != null) {
      l.setMeasureColumnsList(l.getMeasureColumnsList().stream().map(m -> {
        MeasureColumn c = new MeasureColumn();
        c.setMeasureTypeList(m.getMeasureTypeList());
        c.setName(RelCleanser.redactField(m.getName()));
        return c;
      }).collect(Collectors.toList()));
    }
    if (l.getMeasuresList() != null) {
      l.setMeasuresList(l.getMeasuresList().stream().map(RelCleanser::redactField).collect(Collectors.toList()));
    }
    if (l.getSortedColumnsList() != null) {
      l.setSortedColumnsList(l.getSortedColumnsList().stream().map(RelCleanser::redactField).collect(Collectors.toList()));
    }
    if (l.getPartitionedColumnsList() != null) {
      l.setPartitionedColumnsList(l.getPartitionedColumnsList().stream().map(RelCleanser::redactField).collect(Collectors.toList()));
    }
    if (l.getDisplayColumnsList() != null) {
      l.setDisplayColumnsList(l.getDisplayColumnsList().stream().map(RelCleanser::redactField).collect(Collectors.toList()));
    }
    if (!nullOrEmpty(l.getPlan())) {
      l.setPlan(RelCleanser.redactRelTree(l.getPlan()));
    }
    if (!nullOrEmpty(l.getOptimizedPlan())) {
      l.setOptimizedPlan(RelCleanser.redactRelTree(l.getOptimizedPlan()));
    }
    if (l.getNormalizedPlansList() != null) {
      List<String> redactedNormalizedPlansList = l.getNormalizedPlansList().stream().map(RelCleanser::redactRelTree).collect(Collectors.toList());
      l.setNormalizedPlansList(redactedNormalizedPlansList);
    }
    if (l.getNormalizedQueryPlansList() != null) {
      List<String> redactedNormalizedQueryPlansList = l.getNormalizedQueryPlansList().stream().map(RelCleanser::redactRelTree).collect(Collectors.toList());
      l.setNormalizedQueryPlansList(redactedNormalizedQueryPlansList);
    }
    if (l.getSubstitutionsList() != null) {
      l.getSubstitutionsList().forEach(ProfileCleanser::visitSubstitutionProfile);
    }
  }

  private static void visitSubstitutionProfile(SubstitutionProfile substitutionProfile) {
    if (!nullOrEmpty(substitutionProfile.getPlan())) {
      substitutionProfile.setPlan(RelCleanser.redactRelTree(substitutionProfile.getPlan()));
    }
  }

  private static <T> void fromJSON(byte[] data, T message, Schema<T> schema) throws IOException {
    // Configure a parser to intepret non-numeric numbers like NaN correctly
    // although non-standard JSON.
    try(JsonParser parser =  JsonIOUtils
            .newJsonParser(null, data, 0, data.length)
            .enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS)) {
       JsonIOUtils.mergeFrom(parser, message, schema, false);
    }
  }

  private static <T> void toJSON(OutputStream out, T message, Schema<T> schema) throws IOException {
    try (JsonGenerator jsonGenerator =
                  JsonIOUtils.DEFAULT_JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8).disable(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS).useDefaultPrettyPrinter()) {
       JsonIOUtils.writeTo(jsonGenerator, message, schema, false);
    }
  }

  private static boolean nullOrEmpty(String s) {
    return s == null || s.length() == 0;
  }
}
