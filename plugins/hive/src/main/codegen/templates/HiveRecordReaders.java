/*
 * Copyright (C) 2017 Dremio Corporation
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

/**
 * This template is used to generate different Hive record reader classes for different data formats
 * to avoid JIT profile pullusion. These readers are derived from HiveAbstractReader which implements
 * codes for init and setup stage, but the repeated - and performance critical part - next() method is
 * separately implemented in the classes generated from this template. The internal SkipRecordReeader
 * class is also separated as well due to the same reason.
 *
 * As to the performance gain with this change, please refer to:
 * https://issues.apache.org/jira/browse/DRILL-4982
 *
 */
<@pp.dropOutputFile />
<#list hiveFormat.map as entry>
<@pp.changeOutputFile name="//com/dremio/exec/store/hive/exec/Hive${entry.hiveReader}Reader.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store.hive.exec;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.mapred.RecordReader;

import com.dremio.sabot.exec.context.OperatorContext;


public class Hive${entry.hiveReader}Reader extends HiveAbstractReader {

  private Object key;
  private Object value;

  public Hive${entry.hiveReader}Reader(
      ReadDefinition def,
      DatasetSplit split,
      List<SchemaPath> projectedColumns,
      OperatorContext context,
      final HiveConf hiveConf) throws ExecutionSetupException {
    super(def, split, projectedColumns, context, hiveConf);
  }

  public  void internalInit(Properties tableProperties, RecordReader<Object, Object> reader) {
    this.key = reader.createKey();
    this.value = reader.createValue();
  }

  @Override
  public int populateData() throws IOException, SerDeException {
    final RecordReader<Object, Object> reader = this.reader;
    final Converter partTblObjectInspectorConverter = this.partTblObjectInspectorConverter;
    final int numRowsPerBatch = (int) this.numRowsPerBatch;

    final StructField[] selectedStructFieldRefs = this.selectedStructFieldRefs;
    final SerDe partitionSerDe = this.partitionSerDe;
    final StructObjectInspector finalOI = this.finalOI;
    final ObjectInspector[] selectedColumnObjInspectors = this.selectedColumnObjInspectors;
    final HiveFieldConverter[] selectedColumnFieldConverters = this.selectedColumnFieldConverters;
    final ValueVector[] vectors = this.vectors;
    final Object key = this.key;
    final Object value = this.value;
    
    int recordCount = 0;
    while (recordCount < numRowsPerBatch && reader.next(key, value)) {
      Object deSerializedValue = partitionSerDe.deserialize((Writable) value);
      if (partTblObjectInspectorConverter != null) {
        deSerializedValue = partTblObjectInspectorConverter.convert(deSerializedValue);
      }
      for (int i = 0; i < selectedStructFieldRefs.length; i++) {
        Object hiveValue = finalOI.getStructFieldData(deSerializedValue, selectedStructFieldRefs[i]);
        if (hiveValue != null) {
          selectedColumnFieldConverters[i].setSafeValue(selectedColumnObjInspectors[i], hiveValue, vectors[i], recordCount);
        }
      }        
      recordCount++;
    }

    return recordCount;
  }

}
</#list>
