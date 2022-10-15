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
package com.dremio.exec.store.parquet;

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.iceberg.FileContent;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

public class ParquetScanTableFunction extends ScanTableFunction {

  protected ParquetSplitReaderCreatorIterator splitReaderCreatorIterator;
  private RecordReaderIterator recordReaderIterator;
  private VarCharVector inputSplitIdentityPath;
  private ListVector inputDeleteFiles;

  public ParquetScanTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) {
    super(fec, context, props, functionConfig);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    setSplitReaderCreatorIterator();

    if (accessible.getSchema().findFieldIgnoreCase(SystemSchemas.DELETE_FILES).isPresent()) {
      StructVector splitIdentity = (StructVector) getVectorFromSchemaPath(accessible, SystemSchemas.SPLIT_IDENTITY);
      inputSplitIdentityPath = splitIdentity.getChild(SplitIdentity.PATH, VarCharVector.class);
      inputDeleteFiles = (ListVector) getVectorFromSchemaPath(accessible, SystemSchemas.DELETE_FILES);
    }

    return super.setup(accessible);
  }

  @Override
  protected void setIcebergColumnIds(byte[] extendedProperty) {
    splitReaderCreatorIterator.setIcebergExtendedProperty(extendedProperty);
  }

  @Override
  protected RecordReaderIterator createRecordReaderIterator() {
    recordReaderIterator = splitReaderCreatorIterator.getRecordReaderIterator();
    return recordReaderIterator;
  }

  @Override
  protected RecordReaderIterator getRecordReaderIterator() {
    return recordReaderIterator;
  }

  @Override
  protected void addSplits(List<SplitAndPartitionInfo> splits) {
    if (hasIcebergDeleteFiles()) {
      splitReaderCreatorIterator.setDataFileInfoForBatch(getDataFileInfoForBatch());
    }
    splitReaderCreatorIterator.addSplits(splits);
  }

  @Override
  protected void addBoostSplits() throws IOException {
    return;
  }

  protected void setSplitReaderCreatorIterator() throws IOException, ExecutionSetupException {
    splitReaderCreatorIterator = new ParquetSplitReaderCreatorIterator(fec, context, props, functionConfig, false, false);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(super::close, splitReaderCreatorIterator, recordReaderIterator);
  }

  private boolean hasIcebergDeleteFiles() {
    return inputDeleteFiles != null;
  }

  private Map<String, RowLevelDeleteFilterFactory.DataFileInfo> getDataFileInfoForBatch() {
    UnionListReader listReader = inputDeleteFiles.getReader();
    FieldReader pathReader = listReader.reader().reader(SystemSchemas.PATH);
    FieldReader contentReader = listReader.reader().reader(SystemSchemas.FILE_CONTENT);
    FieldReader recordCountReader = listReader.reader().reader(SystemSchemas.RECORD_COUNT);
    FieldReader equalityIdsReader = listReader.reader().reader(SystemSchemas.EQUALITY_IDS);

    Map<String, RowLevelDeleteFilterFactory.DataFileInfo> dataFileInfo = new HashMap<>();

    for (int i = 0; i < incoming.getRecordCount(); i++) {
       String dataFilePath = new String(inputSplitIdentityPath.get(i), StandardCharsets.UTF_8);
      if (dataFileInfo.containsKey(dataFilePath)) {
        // assume 1 row group per split - this will be updated as block splits are expanded
        dataFileInfo.get(dataFilePath).addRowGroups(1);
      } else {
        listReader.setPosition(i);
        List<RowLevelDeleteFilterFactory.DeleteFileInfo> deleteFiles = new ArrayList<>();
        while (listReader.next()) {
          String path = pathReader.readText().toString();
          FileContent content = FileContent.values()[contentReader.readInteger()];
          long recordCount = recordCountReader.readLong();
          List<Integer> equalityIds = null;
          if (equalityIdsReader.isSet()) {
            equalityIds = new ArrayList<>();
            while (equalityIdsReader.next()) {
              equalityIds.add(equalityIdsReader.reader().readInteger());
            }
          }
          deleteFiles.add(new RowLevelDeleteFilterFactory.DeleteFileInfo(path, content, recordCount, equalityIds));

        }

        dataFileInfo.put(dataFilePath, new RowLevelDeleteFilterFactory.DataFileInfo(dataFilePath,
            deleteFiles, 1));
      }
    }

    return dataFileInfo;
  }
}
