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

package com.dremio.exec.store.hive.exec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.google.common.collect.Lists;

public class HiveTextReader extends HiveAbstractReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveTextReader.class);

  private Object key;
  private SkipRecordsInspector skipRecordsInspector;

  public HiveTextReader(HiveTableXattr tableAttr, DatasetSplit split, List<SchemaPath> projectedColumns,
      List<String> partitionColumns, OperatorContext context, final HiveConf hiveConf) throws ExecutionSetupException {
    super(tableAttr, split, projectedColumns, partitionColumns, context, hiveConf);
  }

  @Override
  public void internalInit(Properties tableProperties, RecordReader<Object, Object> reader) {
    key = reader.createKey();
    skipRecordsInspector = new SkipRecordsInspector(tableProperties, reader);
  }

  @Override
  public int populateData() throws IOException, SerDeException {
    final SkipRecordsInspector skipRecordsInspector = this.skipRecordsInspector;
    final RecordReader<Object, Object> reader = this.reader;
    final Converter partTblObjectInspectorConverter = this.partTblObjectInspectorConverter;
    final Object key = this.key;

    final int numRowsPerBatch = (int) this.numRowsPerBatch;

    final StructField[] selectedStructFieldRefs = this.selectedStructFieldRefs;
    final SerDe partitionSerDe = this.partitionSerDe;
    final StructObjectInspector finalOI = this.finalOI;
    final ObjectInspector[] selectedColumnObjInspectors = this.selectedColumnObjInspectors;
    final HiveFieldConverter[] selectedColumnFieldConverters = this.selectedColumnFieldConverters;
    final ValueVector[] vectors = this.vectors;

    skipRecordsInspector.reset();
    Object value;

    int recordCount = 0;

    while (recordCount < numRowsPerBatch && reader.next(key, value = skipRecordsInspector.getNextValue())) {
      if (skipRecordsInspector.doSkipHeader(recordCount++)) {
        continue;
      }
      Object bufferedValue = skipRecordsInspector.bufferAdd(value);
      if (bufferedValue != null) {
        Object deSerializedValue = partitionSerDe.deserialize((Writable) bufferedValue);
        if (partTblObjectInspectorConverter != null) {
          deSerializedValue = partTblObjectInspectorConverter.convert(deSerializedValue);
        }

        for (int i = 0; i < selectedStructFieldRefs.length; i++) {
          Object hiveValue = finalOI.getStructFieldData(deSerializedValue, selectedStructFieldRefs[i]);
          if (hiveValue != null) {
            selectedColumnFieldConverters[i].setSafeValue(selectedColumnObjInspectors[i], hiveValue, vectors[i], skipRecordsInspector.getActualCount());
          }
        }
        skipRecordsInspector.incrementActualCount();
      }
      skipRecordsInspector.incrementTempCount();
    }

    skipRecordsInspector.updateContinuance();
    return skipRecordsInspector.getActualCount();
  }

  /**
   * SkipRecordsInspector encapsulates logic to skip header and footer from
   * file. Logic is applicable only for predefined in constructor file formats.
   */
  private static final class SkipRecordsInspector {

    private final Set<Object> fileFormats;
    private int headerCount;
    private int footerCount;
    private Queue<Object> footerBuffer;
    // indicates if we continue reading the same file
    private boolean continuance;
    private int holderIndex;
    private List<Object> valueHolder;
    private int actualCount;
    // actualCount without headerCount, used to determine holderIndex
    private int tempCount;

    protected SkipRecordsInspector(Properties tableProperties, RecordReader reader) {
      this.fileFormats = new HashSet<Object>(Arrays.asList(org.apache.hadoop.mapred.TextInputFormat.class.getName()));
      this.headerCount = retrievePositiveIntProperty(tableProperties, serdeConstants.HEADER_COUNT, 0);
      this.footerCount = retrievePositiveIntProperty(tableProperties, serdeConstants.FOOTER_COUNT, 0);
      logger.debug("skipRecordInspector: fileFormat {}, headerCount {}, footerCount {}", this.fileFormats,
          this.headerCount, this.footerCount);
      this.footerBuffer = Lists.newLinkedList();
      this.continuance = false;
      this.holderIndex = -1;
      this.valueHolder = initializeValueHolder(reader, footerCount);
      this.actualCount = 0;
      this.tempCount = 0;
    }

    protected boolean doSkipHeader(int recordCount) {
      return !continuance && recordCount < headerCount;
    }

    protected void reset() {
      tempCount = holderIndex + 1;
      actualCount = 0;
      if (!continuance) {
        footerBuffer.clear();
      }
    }

    protected Object bufferAdd(Object value) throws SerDeException {
      footerBuffer.add(value);
      if (footerBuffer.size() <= footerCount) {
        return null;
      }
      return footerBuffer.poll();
    }

    protected Object getNextValue() {
      holderIndex = tempCount % getHolderSize();
      return valueHolder.get(holderIndex);
    }

    private int getHolderSize() {
      return valueHolder.size();
    }

    protected void updateContinuance() {
      this.continuance = actualCount != 0;
    }

    protected int incrementTempCount() {
      return ++tempCount;
    }

    protected int getActualCount() {
      return actualCount;
    }

    protected int incrementActualCount() {
      return ++actualCount;
    }

    /**
     * Retrieves positive numeric property from Properties object by name.
     * Return default value if 1. file format is absent in predefined file
     * formats list 2. property doesn't exist in table properties 3. property
     * value is negative otherwise casts value to int.
     *
     * @param tableProperties
     *          property holder
     * @param propertyName
     *          name of the property
     * @param defaultValue
     *          default value
     * @return property numeric value
     * @throws NumberFormatException
     *           if property value is non-numeric
     */
    protected int retrievePositiveIntProperty(Properties tableProperties, String propertyName, int defaultValue) {
      int propertyIntValue = defaultValue;
      if (!fileFormats.contains(tableProperties.get(hive_metastoreConstants.FILE_INPUT_FORMAT))) {
        return propertyIntValue;
      }
      Object propertyObject = tableProperties.get(propertyName);
      if (propertyObject != null) {
        try {
          propertyIntValue = Integer.valueOf((String) propertyObject);
        } catch (NumberFormatException e) {
          throw new NumberFormatException(String.format("Hive table property %s value '%s' is non-numeric",
              propertyName, propertyObject.toString()));
        }
      }
      return propertyIntValue < 0 ? defaultValue : propertyIntValue;
    }

    /**
     * Creates buffer of objects to be used as values, so these values can be
     * re-used. Objects number depends on number of lines to skip in the end of
     * the file plus one object.
     *
     * @param reader
     *          RecordReader to return value object
     * @param skipFooterLines
     *          number of lines to skip at the end of the file
     * @return list of objects to be used as values
     */
    private List<Object> initializeValueHolder(RecordReader reader, int skipFooterLines) {
      List<Object> valueHolder = new ArrayList<>(skipFooterLines + 1);
      for (int i = 0; i <= skipFooterLines; i++) {
        valueHolder.add(reader.createValue());
      }
      return valueHolder;
    }
  }

}
