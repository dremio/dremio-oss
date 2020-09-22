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
package com.dremio.exec.planner;

import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestCorrelatedQuery extends BaseTestQuery {
  @Test
  public void testCorrelatedQuery() throws Exception {
    String query = "SELECT * FROM cp.\"employee.json\" e1 where not (EXISTS (select * from cp.\"employee.json\" e2 where e1.employee_id=e2.employee_id))";
    test(query);
  }

  @Test // DX-20910
  public void testCorrelatedQueryWithLimit() throws Exception {
    String query = "SELECT * FROM cp.\"employee.json\" e1 where not (EXISTS (select * from cp.\"employee.json\" e2 where e1.employee_id=e2.employee_id LIMIT 1))";
    test(query);
  }
}
