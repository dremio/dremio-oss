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

package com.dremio.exec.fn.impl;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestMultiInputAdd extends BaseTestQuery {

//    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestMathFunctions.class);


    @Test
    public void testMultiInputAdd() throws Exception {
      List<QueryDataBatch> results = client.runQuery(com.dremio.exec.proto.UserBitShared.QueryType.PHYSICAL,
              Files.toString(FileUtils.getResourceAsFile("/functions/multi_input_add_test.json"), Charsets.UTF_8));
      try(RecordBatchLoader batchLoader = new RecordBatchLoader(nodes[0].getContext().getAllocator())){
        QueryDataBatch batch = results.get(0);
        assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

        for (VectorWrapper<?> v : batchLoader) {

            ValueVector.Accessor accessor = v.getValueVector().getAccessor();

            assertTrue((accessor.getObject(0)).equals(10));
        }

        batchLoader.clear();
        for(QueryDataBatch b : results){
            b.release();
        }
      }
    }
}
