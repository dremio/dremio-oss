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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import org.junit.Assert;
import org.junit.Test;

public class TestMetadataRefreshForParquet extends BaseTestQuery {

  private static final String DATAFILE = "cp.\"store/parquet/complex/complex.parquet\"";

  @Test
  public void columnLimitExceeded() throws Exception {
    final String query =
        String.format(
            "select t1.amount, t1.\"date\", t1.marketing_info, t1.\"time\", t1.trans_id, t1.trans_info, t1.user_info "
                + "from %s t1",
            DATAFILE);

    try (AutoCloseable ignored = setMaxLeafColumns(2)) {
      test(query);
      fail("query should have failed");
    } catch (UserException e) {
      assertEquals(e.getErrorType(), UserBitShared.DremioPBError.ErrorType.VALIDATION);
      Assert.assertTrue(e.getMessage().contains("exceeded the maximum number of fields of 2"));
    }
  }
}
