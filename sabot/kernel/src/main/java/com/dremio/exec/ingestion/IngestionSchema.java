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
package com.dremio.exec.ingestion;

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.DirList;

public class IngestionSchema {

  public static final String PIPE_NAME = "pipeName";
  public static final String PIPE_ID = "pipeId";
  public static final String REQUEST_ID = "requestId";
  public static final String INGESTION_SOURCE_TYPE = "ingestionSourceType";
  public static final String MODIFICATION_TIME = DirList.OUTPUT_SCHEMA.MODIFICATION_TIME;

  public static final BatchSchema BATCH_SCHEMA =
      BatchSchema.newBuilder()
          .addField(
              MajorTypeHelper.getFieldForNameAndMajorType(
                  PIPE_NAME, Types.optional(MinorType.VARCHAR)))
          .addField(
              MajorTypeHelper.getFieldForNameAndMajorType(
                  PIPE_ID, Types.optional(MinorType.VARCHAR)))
          .addField(
              MajorTypeHelper.getFieldForNameAndMajorType(
                  REQUEST_ID, Types.optional(MinorType.VARCHAR)))
          .addField(
              MajorTypeHelper.getFieldForNameAndMajorType(
                  INGESTION_SOURCE_TYPE, Types.optional(MinorType.VARCHAR)))
          .addField(
              DirList.OUTPUT_SCHEMA.BATCH_SCHEMA.findField(DirList.OUTPUT_SCHEMA.MODIFICATION_TIME))
          .setSelectionVectorMode(SelectionVectorMode.NONE)
          .build();
}
