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
package com.dremio.sabot.op.screen;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.WritableBatch;
import com.dremio.sabot.exec.context.OperatorContext;
import org.apache.arrow.memory.BufferAllocator;

public class VectorRecordMaterializer implements RecordMaterializer {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VectorRecordMaterializer.class);

  private QueryId queryId;
  private VectorAccessible incoming;
  private BufferAllocator allocator;

  public VectorRecordMaterializer(OperatorContext context, VectorAccessible incoming) {
    this.queryId = context.getFragmentHandle().getQueryId();
    this.incoming = incoming;
    this.allocator = context.getAllocator();
    assert incoming.getSchema() != null : "Schema must be defined.";
  }

  @Override
  public QueryWritableBatch convertNext(int count) {

    final WritableBatch w = WritableBatch.get(incoming).transfer(allocator);

    QueryData header =
        QueryData.newBuilder() //
            .setQueryId(queryId) //
            .setRowCount(count) //
            .setDef(w.getDef())
            .build();
    QueryWritableBatch batch = new QueryWritableBatch(header, w.getBuffers());
    return batch;
  }
}
