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
package com.dremio.exec.store.ischema;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestInfoSchemaFilterPushDown extends PlanTestBase {

  @Test
  public void testFilterPushdown_Equal() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA='INFORMATION_SCHEMA'";
    final String scan = "query=[equals {   field: \"SEARCH_SCHEMA\"   stringValue: \"INFORMATION_SCHEMA\" } ]";

    testHelper(query, scan, false);
  }

  @Ignore("NOT not currently supported")
  @Test
  public void testFilterPushdown_NonEqual() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'";
    final String scan = "query=[type: NOT not {   clause {     type: TERM     term {       field: \"SEARCH_SCHEMA\"       value: \"INFORMATION_SCHEMA\"     }   } } ]";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_Like() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA LIKE '%SCH%'";
    final String scan = "query=[like {   field: \"SEARCH_SCHEMA\"   pattern: \"%SCH%\" } ]";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_LikeWithEscape() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA LIKE '%\\\\SCH%' ESCAPE '\\'";
    final String scan = "query=[like {   field: \"SEARCH_SCHEMA\"   pattern: \"%\\\\\\\\SCH%\"   escape: \"\\\\\" } ])";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_And() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' AND " +
        "TABLE_NAME <> 'version'";
    final String scan = "query=[equals {   field: \"SEARCH_SCHEMA\"   stringValue: \"sys\" } ]";

    testHelper(query, scan, true);
  }

  @Ignore("NOT not currently supported")
  @Test
  public void testFilterPushdown_Or() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' OR " +
        "TABLE_NAME <> 'version' OR " +
        "TABLE_SCHEMA like '%sdfgjk%'";
    final String scan = "query=[type: BOOLEAN boolean {   op: OR   clauses {     type: TERM     term {       field: \"SEARCH_SCHEMA\"       value: \"sys\"     }   }   clauses {     type: NOT     not {       clause {         type: TERM         term {           field: \"SEARCH_NAME\"           value: \"version\"         }       }     }   }   clauses {     type: WILDCARD     wildcard {       field: \"SEARCH_SCHEMA\"       value: \"*sdfgjk*\"     }   } } ]";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushDownWithProject_Equal() throws Exception {
    final String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.\"COLUMNS\" WHERE TABLE_SCHEMA = 'INFORMATION_SCHEMA'";
    final String scan = "query=[equals {   field: \"SEARCH_SCHEMA\"   stringValue: \"INFORMATION_SCHEMA\" } ]";
    testHelper(query, scan, false);
  }

  @Ignore("NOT not currently supported")
  @Test
  public void testFilterPushDownWithProject_NotEqual() throws Exception {
    final String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.\"COLUMNS\" WHERE TABLE_NAME <> 'TABLES'";
    final String scan = "query=[type: NOT not {   clause {     type: TERM     term {       field: \"SEARCH_NAME\"       value: \"TABLES\"     }   } } ]";
    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushDownWithProject_Like() throws Exception {
    final String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.\"COLUMNS\" WHERE TABLE_NAME LIKE '%BL%'";
    final String scan = "query=[like {   field: \"SEARCH_NAME\"   pattern: \"%BL%\" } ]";
    testHelper(query, scan, false);
  }

  @Test
  public void testPartialFilterPushDownWithProject() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' AND " +
        "TABLE_NAME = 'version' AND " +
        "COLUMN_NAME like 'commit%s' AND " + // this is not expected to pushdown into scan
        "IS_NULLABLE = 'YES'"; // this is not expected to pushdown into scan
    final String scan = "query=[and {   clauses {     equals {       field: \"SEARCH_SCHEMA\"       stringValue: \"sys\"     }   }   clauses {     equals {       field: \"SEARCH_NAME\"       stringValue: \"version\"     }   } } ]";
    testHelper(query, scan, true);
  }


  private void testHelper(final String query, String filterInScan, boolean filterPrelExpected) throws Exception {
    final String plan = getPlanInString("EXPLAIN PLAN FOR " + query, OPTIQ_FORMAT);

    if (!filterPrelExpected) {
      // If filter prel is not expected, make sure it is not in plan
      assertFalse(String.format("Expected plan to not contain filter, however it did.\n %s", plan), plan.contains("Filter("));
    } else {
      assertTrue(String.format("Expected plan to contain filter and did not.\n %s", plan), plan.contains("Filter("));
    }

    // Check for filter pushed into scan.
    assertTrue(String.format("Expected plan to contain %s, however it did not.\n %s", filterInScan, plan), plan.contains(filterInScan));

    // run the query
    test(query);
  }
}
