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

import static com.dremio.exec.store.RecordReader.COL_IDS;
import static com.dremio.exec.store.RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;
import static com.dremio.exec.store.RecordReader.SPLIT_IDENTITY;
import static com.dremio.exec.store.RecordReader.SPLIT_INFORMATION;
import static com.dremio.exec.store.SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_TYPE;
import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.RecordSet.st;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.RecordBatchValidatorDefaultImpl;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.RecordSet.Tuple;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.collect.Lists;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

public class TestManifestFileDuplicateRemoveTableFunction extends BaseTestTableFunction {
  private static final BatchSchema INPUT_SCHEMA =
      BatchSchema.newBuilder()
          .addField(Field.nullable(FILE_PATH, Types.MinorType.VARCHAR.getType()))
          .addField(Field.nullable(FILE_TYPE, Types.MinorType.VARCHAR.getType()))
          .addField(
              new Field(
                  SPLIT_IDENTITY,
                  FieldType.nullable(Types.MinorType.STRUCT.getType()),
                  Lists.newArrayList(
                      Field.nullable(SplitIdentity.PATH, Types.MinorType.VARCHAR.getType()),
                      Field.nullable(SplitIdentity.OFFSET, Types.MinorType.BIGINT.getType()),
                      Field.nullable(SplitIdentity.LENGTH, Types.MinorType.BIGINT.getType()),
                      Field.nullable(SplitIdentity.FILE_LENGTH, Types.MinorType.BIGINT.getType()))))
          .addField(Field.nullable(SPLIT_INFORMATION, Types.MinorType.VARBINARY.getType()))
          .addField(Field.nullable(COL_IDS, Types.MinorType.VARBINARY.getType()))
          .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
          .build();
  private static final BatchSchema OUTPUT_SCHEMA = INPUT_SCHEMA;

  private static final String MANIFEST_FILE_PATH_1 = "file:///manifestpath1";
  private static final String MANIFEST_FILE_PATH_2 = "file:///manifestpath2";
  private static final String MANIFEST_LIST_FILE_PATH_1 = "file:///snap-manifestlistpath1";

  private static final String MANIFEST_TYPE = IcebergFileType.MANIFEST.name();
  private static final String MANIFEST_LIST_TYPE = IcebergFileType.MANIFEST_LIST.name();

  private static final Tuple SPLIT_IDENTITY_1 = st(MANIFEST_FILE_PATH_1, 0L, 50L, 50L);
  private static final Tuple SPLIT_IDENTITY_2 = st(MANIFEST_FILE_PATH_2, 0L, 50L, 50L);

  private static final byte[] COL_IDS_1 = new byte[] {1};
  private static final byte[] COL_IDS_2 = new byte[] {2};
  private static final byte[] SPLIT_INFO_1 = new byte[] {1};
  private static final byte[] SPLIT_INFO_2 = new byte[] {1};

  @Test
  public void testDuplicateManifestFilePathRemove() throws Exception {
    RecordSet input =
        rs(
            INPUT_SCHEMA,
            r(MANIFEST_FILE_PATH_1, MANIFEST_TYPE, SPLIT_IDENTITY_1, SPLIT_INFO_1, COL_IDS_1),
            r(MANIFEST_FILE_PATH_1, MANIFEST_TYPE, SPLIT_IDENTITY_1, SPLIT_INFO_1, COL_IDS_1),
            r(MANIFEST_FILE_PATH_2, MANIFEST_TYPE, SPLIT_IDENTITY_2, SPLIT_INFO_2, COL_IDS_2),
            r(MANIFEST_FILE_PATH_2, MANIFEST_TYPE, SPLIT_IDENTITY_2, SPLIT_INFO_2, COL_IDS_2),
            r(MANIFEST_LIST_FILE_PATH_1, MANIFEST_LIST_TYPE, null, null, null));

    RecordSet output =
        rs(
            OUTPUT_SCHEMA,
            r(null, null, SPLIT_IDENTITY_1, SPLIT_INFO_1, COL_IDS_1),
            r(null, null, SPLIT_IDENTITY_2, SPLIT_INFO_2, COL_IDS_2),
            r(MANIFEST_LIST_FILE_PATH_1, MANIFEST_LIST_TYPE, null, null, null));

    validateSingle(
        getPop(),
        TableFunctionOperator.class,
        input,
        new RecordBatchValidatorDefaultImpl(output),
        3);
  }

  private TableFunctionPOP getPop() {
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.MANIFEST_FILE_DUPLICATE_REMOVE,
            true,
            new TableFunctionContext(
                null,
                SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA),
                null,
                null,
                null,
                null,
                null,
                null,
                SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA
                    .merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA)
                    .getFields()
                    .stream()
                    .map(f -> SchemaPath.getSimplePath(f.getName()))
                    .collect(Collectors.toList()),
                null,
                null,
                false,
                false,
                false,
                null)));
  }
}
