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
<@pp.dropOutputFile />
<@pp.changeOutputFile name="/com/dremio/exec/store/RowBasedRecordWriter.java" />
<#include "/@includes/license.ftl" />

package com.dremio.exec.store;

import org.apache.arrow.vector.complex.reader.FieldReader;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.EventBasedRecordWriter.FieldConverter;
import com.dremio.exec.store.RecordWriter.OutputEntryListener;
import org.apache.arrow.vector.complex.reader.FieldReader;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.util.Map;

/**
 * Extension of RecordWriter for row based writer output. If the writer output format requires row based
 * accessing of records, then it should implement this interface.
 */
public abstract class RowBasedRecordWriter implements RecordWriter {

  protected VectorAccessible incoming;
  protected OutputEntryListener listener;
  protected WriteStatsListener writeStatsListener;

  private EventBasedRecordWriter eventBasedRecordWriter;
  
  public final void setup(final VectorAccessible incoming, OutputEntryListener listener,
                          WriteStatsListener statsListener) throws IOException {
    this.incoming = incoming;
    this.listener = listener;
    this.writeStatsListener = statsListener;
    setup();
  }
  
  public abstract void setup() throws IOException;
  
  @Override
  public final int writeBatch(int offset, int length) throws IOException {
    if (this.eventBasedRecordWriter == null) {
      this.eventBasedRecordWriter = new EventBasedRecordWriter(incoming, this);
    }
    return eventBasedRecordWriter.write(offset, length);
  }

  /**
   * Called before starting writing fields in a record.
   * @throws IOException
   */
  public abstract void startRecord() throws IOException;

  /** Add the field value given in <code>valueHolder</code> at the given column number <code>fieldId</code>. */
  public abstract FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader);
  public abstract FieldConverter getNewListConverter(int fieldId, String fieldName, FieldReader reader);
  public abstract FieldConverter getNewUnionConverter(int fieldId, String fieldName, FieldReader reader);
  public abstract FieldConverter getNewNullConverter(int fieldId, String fieldName, FieldReader reader);

<#list vv.types as type>
  <#list type.minor as minor>
  <#assign typeMapping = TypeMappings[minor.class]!{}>
  <#assign supported = typeMapping.supported!true>
  <#if supported>
  /** Add the field value given in <code>valueHolder</code> at the given column number <code>fieldId</code>. */
  public abstract FieldConverter getNewNullable${minor.class}Converter(int fieldId, String fieldName, FieldReader reader);
  </#if>
  </#list>
</#list>

  /**
   * Called after adding all fields in a particular record are added using add{TypeHolder}(fieldId, TypeHolder) methods.
   * @throws IOException
   */
  public abstract void endRecord() throws IOException;
}
