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
package com.dremio.plugins.elastic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.DremioTestWrapper;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.plugins.elastic.ElasticBaseTestQuery.ShowIDColumn;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.dremio.sabot.rpc.user.QueryDataBatch;

@ShowIDColumn(enabled=true)
public class TestMetadataColumns extends ElasticBaseTestQuery {

  protected String TABLENAME;
  protected String[] ids = new String[5];
  protected String[] uids = new String[5];

  @Before
  public final void loadTable() throws Exception {
    ColumnData[] data = getBusinessData();
    load(schema, table, data);

    TABLENAME = "elasticsearch." + schema + "." + table;

    // Figure out the id/uid combos for this table, so that we can write some queries
    List<QueryDataBatch> queryBatch = testRunAndReturn(QueryType.SQL, "select _id, _uid, business_id from " + TABLENAME);
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    List<Map<String, Object>> records = new ArrayList<>();
    DremioTestWrapper.addToMaterializedResults(records, queryBatch, loader);

    boolean first12345 = true;
    for (Map<String, Object> record : records) {
      String bid = (String) record.get("`business_id`");
      String id = (String) record.get("`" + ElasticsearchConstants.ID + "`");
      String uid = (String) record.get("`" + ElasticsearchConstants.UID + "`");
      switch (bid) {
        case "12345":
          if (first12345) {
            ids[0] = id;
            uids[0] = uid;
            first12345 = false;
          } else {
            ids[3] = id;
            uids[3] = uid;
          }
          break;
        case "abcde":
          ids[1] = id;
          uids[1] = uid;
          break;
        case "7890":
          ids[2] = id;
          uids[2] = uid;
          break;
        case "xyz":
          ids[4] = id;
          uids[4] = uid;
          break;
        default:
          assert false;
      }
    }
  }

  @Test
  public final void testSimpleScan() throws Exception {
    final String sql = "select _id, _uid, _type, _index from " + TABLENAME;
    verifyJsonInPlan(sql, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\", \"_index\", \"_type\", \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("_id", "_uid", "_type", "_index")
      .baselineValues(ids[0], uids[0], table, schema)
      .baselineValues(ids[1], uids[1], table, schema)
      .baselineValues(ids[2], uids[2], table, schema)
      .baselineValues(ids[3], uids[3], table, schema)
      .baselineValues(ids[4], uids[4], table, schema)
      .go();
  }

  @Test
  public final void testProjectIndexType() throws Exception {
    final String sqlQuery = "select CHAR_LENGTH(_index), CHAR_LENGTH(_type) from " + TABLENAME;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_index\", \"_type\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1")
      .baselineValues(schema.length(), table.length())
      .baselineValues(schema.length(), table.length())
      .baselineValues(schema.length(), table.length())
      .baselineValues(schema.length(), table.length())
      .baselineValues(schema.length(), table.length())
      .go();
  }

  @Test
  public final void testProjectID() throws Exception {
    final String sqlQuery = "select CHAR_LENGTH(_id) from " + TABLENAME;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(ids[0].length())
      .baselineValues(ids[1].length())
      .baselineValues(ids[2].length())
      .baselineValues(ids[3].length())
      .baselineValues(ids[4].length())
      .go();
  }

  @Test
  public final void testProjectUID() throws Exception {
    final String sqlQuery = "select CHAR_LENGTH(_uid) from " + TABLENAME;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(uids[0].length())
      .baselineValues(uids[1].length())
      .baselineValues(uids[2].length())
      .baselineValues(uids[3].length())
      .baselineValues(uids[4].length())
      .go();
  }

  @Test
  public final void testFilterIDOrIsNull() throws Exception {
    final String cond1 = "_id = '" + ids[1] + "' or _id is null";
    final String sqlQuery = "select _id from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_id")
      .baselineValues(ids[1])
      .go();
  }

  @Test
  public final void testFilterID() throws Exception {
    final String cond1 = "_id = '" + ids[1] + "'";
    final String cond2 = "_id = '" + ids[2] + "'";
    final String cond3 = "_id = '" + ids[4] + "'";
    final String sqlQuery = "select _id from " + TABLENAME + " where ( " + cond1 + " OR " + cond2 + ") OR (" + cond3 + " OR " + cond1 + ")";
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"should\" : [ {\n" +
      "        \"match\" : {\n" +
      "          \"_id\" : {\n" +
      "            \"query\" : \""+ids[1]+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"match\" : {\n" +
      "          \"_id\" : {\n" +
      "            \"query\" : \""+ids[2]+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"match\" : {\n" +
      "          \"_id\" : {\n" +
      "            \"query\" : \""+ids[4]+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_id")
      .baselineValues(ids[1])
      .baselineValues(ids[2])
      .baselineValues(ids[4])
      .go();
  }

  @Test
  public final void testFilterIDAndIsNotNull() throws Exception {
    final String cond1 = "_id = '" + ids[1] + "' and _id is not null";
    final String cond2 = "_id = '" + ids[2] + "' and _id is not null";
    final String cond3 = "_id = '" + ids[4] + "' and _id is not null";
    final String sqlQuery = "select _id from " + TABLENAME + " where ( " + cond1 + " OR " + cond2 + ") OR (" + cond3 + " OR " + cond1 + ")";
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_id")
      .baselineValues(ids[1])
      .baselineValues(ids[2])
      .baselineValues(ids[4])
      .go();
  }

  @Test
  public final void testFilterUIDOrIsNull() throws Exception {
    final String cond1 = "_uid = '" + uids[1] + "' or _uid is null";
    final String sqlQuery = "select _uid from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"should\" : [ {\n" +
      "        \"match\" : {\n" +
      "          \"_uid\" : {\n" +
      "            \"query\" : \"" + uids[1] + "\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"bool\" : {\n" +
      "          \"must_not\" : {\n" +
      "            \"exists\" : {\n" +
      "              \"field\" : \"_uid\"\n" +
      "            }\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_uid")
      .baselineValues(uids[1])
      .go();
  }

  @Test
  public final void testFilterUID() throws Exception {
    final String cond1 = "_uid = '" + uids[1] + "'";
    final String cond2 = "_uid = '" + uids[2] + "'";
    final String cond3 = "_uid = '" + uids[4] + "'";
    final String sqlQuery = "select _uid from " + TABLENAME + " where ( " + cond1 + " OR " + cond2 + ") OR (" + cond3 + " OR " + cond1 + ")";
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"should\" : [ {\n" +
      "        \"match\" : {\n" +
      "          \"_uid\" : {\n" +
      "            \"query\" : \""+uids[1]+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"match\" : {\n" +
      "          \"_uid\" : {\n" +
      "            \"query\" : \""+uids[2]+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"match\" : {\n" +
      "          \"_uid\" : {\n" +
      "            \"query\" : \""+uids[4]+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_uid")
      .baselineValues(uids[1])
      .baselineValues(uids[2])
      .baselineValues(uids[4])
      .go();
  }

  @Test
  public final void testFilterIndexAndType() throws Exception {
    final String cond1 = "_index = '" + schema + "' OR _type = '" + table + "'";
    final String sqlQuery = "select _index, _type, _id from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"should\" : [ {\n" +
      "        \"match\" : {\n" +
      "          \"_index\" : {\n" +
      "            \"query\" : \""+schema+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"match\" : {\n" +
      "          \"_type\" : {\n" +
      "            \"query\" : \""+table+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\", \"_index\", \"_type\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_index", "_type", "_id")
      .baselineValues(schema, table, ids[0])
      .baselineValues(schema, table, ids[1])
      .baselineValues(schema, table, ids[2])
      .baselineValues(schema, table, ids[3])
      .baselineValues(schema, table, ids[4])
      .go();
  }

  @Test
  public final void testFilterTypeOrIsNull() throws Exception {
    final String cond1 = "_type = '" + table + "' or _type is null";
    final String sqlQuery = "select _index, _type, _id from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"should\" : [ {\n" +
      "        \"match\" : {\n" +
      "          \"_type\" : {\n" +
      "            \"query\" : \""+table+"\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"bool\" : {\n" +
      "          \"must_not\" : {\n" +
      "            \"exists\" : {\n" +
      "              \"field\" : \"_type\"\n" +
      "            }\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\", \"_index\", \"_type\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_index", "_type", "_id")
      .baselineValues(schema, table, ids[0])
      .baselineValues(schema, table, ids[1])
      .baselineValues(schema, table, ids[2])
      .baselineValues(schema, table, ids[3])
      .baselineValues(schema, table, ids[4])
      .go();
  }

  @Test
  public final void testFilterIndexOrIsNull() throws Exception {
    final String cond1 = "_index  = '" + schema + "' or _index is null";
    final String sqlQuery = "select _index, _type, _id from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\", \"_index\", \"_type\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_index", "_type", "_id")
      .baselineValues(schema, table, ids[0])
      .baselineValues(schema, table, ids[1])
      .baselineValues(schema, table, ids[2])
      .baselineValues(schema, table, ids[3])
      .baselineValues(schema, table, ids[4])
      .go();
  }

  @Test
  public final void testFilterTypeAndIsNotNull() throws Exception {
    final String cond1 = "_type = '" + table + "' and _type is not null";
    final String sqlQuery = "select _index, _type, _id from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match\" : {\n" +
      "      \"_type\" : {\n" +
      "        \"query\" : \"" + table + "\",\n" +
      "        \"type\" : \"boolean\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\", \"_index\", \"_type\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_index", "_type", "_id")
      .baselineValues(schema, table, ids[0])
      .baselineValues(schema, table, ids[1])
      .baselineValues(schema, table, ids[2])
      .baselineValues(schema, table, ids[3])
      .baselineValues(schema, table, ids[4])
      .go();
  }


  @Test
  public final void testFilterIndexAndIsNotNull() throws Exception {
    final String cond1 = "_index = '" + schema + "' and _index is not null";
    final String sqlQuery = "select _index, _type, _id from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match\" : {\n" +
      "      \"_index\" : {\n" +
      "        \"query\" : \""+schema+"\",\n" +
      "        \"type\" : \"boolean\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\", \"_index\", \"_type\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_index", "_type", "_id")
      .baselineValues(schema, table, ids[0])
      .baselineValues(schema, table, ids[1])
      .baselineValues(schema, table, ids[2])
      .baselineValues(schema, table, ids[3])
      .baselineValues(schema, table, ids[4])
      .go();
  }

  @Test
  public final void testFilterUIDAndIsNotNull() throws Exception {
    final String cond1 = "_uid = '" + uids[1] + "' and _uid is not null";
    final String cond2 = "_uid = '" + uids[2] + "' and _uid is not null";
    final String cond3 = "_uid = '" + uids[4] + "' and _uid is not null";
    final String sqlQuery = "select _uid from " + TABLENAME + " where ( " + cond1 + " OR " + cond2 + ") OR (" + cond3 + " OR " + cond1 + ")";
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"should\" : [ {\n" +
      "        \"bool\" : {\n" +
      "          \"must\" : [ {\n" +
      "            \"match\" : {\n" +
      "              \"_uid\" : {\n" +
      "                \"query\" : \""+uids[1]+"\",\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"exists\" : {\n" +
      "              \"field\" : \"_uid\"\n" +
      "            }\n" +
      "          } ]\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"bool\" : {\n" +
      "          \"must\" : [ {\n" +
      "            \"match\" : {\n" +
      "              \"_uid\" : {\n" +
      "                \"query\" : \""+uids[2]+"\",\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"exists\" : {\n" +
      "              \"field\" : \"_uid\"\n" +
      "            }\n" +
      "          } ]\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"bool\" : {\n" +
      "          \"must\" : [ {\n" +
      "            \"match\" : {\n" +
      "              \"_uid\" : {\n" +
      "                \"query\" : \""+uids[4]+"\",\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"exists\" : {\n" +
      "              \"field\" : \"_uid\"\n" +
      "            }\n" +
      "          } ]\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_uid")
      .baselineValues(uids[1])
      .baselineValues(uids[2])
      .baselineValues(uids[4])
      .go();
  }

  @Test
  public final void testFilterIDLike() throws Exception {
    final String cond1 = "_id LIKE '%" + ids[1] + "%'";
    final String sqlQuery = "select _id from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_id")
      .baselineValues(ids[1])
      .go();
  }

  @Ignore("DX-7888")
  @Test
  public final void testFilterUIDLike() throws Exception {
    final String cond1 = "_uid LIKE '%" + uids[1] + "%'";
    final String sqlQuery = "select _uid from " + TABLENAME + " where " + cond1;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_uid")
      .baselineValues(uids[1])
      .go();
  }

  @Test
  public final void testFilterIndexTypeLike() throws Exception {
    final String cond1 = "_index LIKE '%" + schema + "%'";
    final String cond2 = "_type LIKE '%" + table + "%'";
    final String sqlQuery = "select _uid, _id, _index, _type from " + TABLENAME + " where " + cond1 + " OR " + cond2;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\", \"_index\", \"_type\", \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_uid", "_id", "_index", "_type")
      .baselineValues(uids[0], ids[0], schema, table)
      .baselineValues(uids[1], ids[1], schema, table)
      .baselineValues(uids[2], ids[2], schema, table)
      .baselineValues(uids[3], ids[3], schema, table)
      .baselineValues(uids[4], ids[4], schema, table)
      .go();
  }

  @Test
  public final void testFilterIDContains() throws Exception {
    final String sqlQuery = "select _id from " + TABLENAME + " where contains(_id:" + ids[1] + ")";
    verifyJsonInPlanHelper(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"query_string\" : {\n" +
      "      \"query\" : \"_id : "+ids[1]+"\"\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_id\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"},
      true);
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_id")
      .baselineValues(ids[1])
      .go();
  }

  @Test
  public final void testFilterUIDContains() throws Exception {
    final String sqlQuery = "select _uid from " + TABLENAME + " where contains(_uid:" + uids[1] + ")";
    verifyJsonInPlanHelper(sqlQuery, new String[] {
        "=[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"query_string\" : {\n" +
        "      \"query\" : \"_uid : "+uids[1]+"\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"_uid\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"},
      true);
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_uid")
      .baselineValues(uids[1])
      .go();
  }

  @Test
  public final void testFilterIndexTypeContains() throws Exception {
    final String sqlQuery = "select _uid, _id, _index, _type from " + TABLENAME + " where contains(_index:" + schema + ", _type:" + table + ")";
    verifyJsonInPlanHelper(sqlQuery, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"query_string\" : {\n" +
        "      \"query\" : \"_index : " + schema + ",  _type : "+table+"\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"_id\", \"_index\", \"_type\", \"_uid\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"},
      true);
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_uid", "_id", "_index", "_type")
      .baselineValues(uids[0], ids[0], schema, table)
      .baselineValues(uids[1], ids[1], schema, table)
      .baselineValues(uids[2], ids[2], schema, table)
      .baselineValues(uids[3], ids[3], schema, table)
      .baselineValues(uids[4], ids[4], schema, table)
      .go();
  }

  @Test
  public final void testFilterUIDItem() throws Exception {
    final String sqlQuery = "select t._uid from " + TABLENAME + " t where t._uid.something > 3";
    errorTypeTestHelper(sqlQuery, ErrorType.VALIDATION);
  }

  @Test
  public final void testFilterIDItem() throws Exception {
    final String sqlQuery = "select t._id from " + TABLENAME + " t where t._id.something > 3";
    errorTypeTestHelper(sqlQuery, ErrorType.VALIDATION);
  }

  @Test
  public final void testFilterTypeItem() throws Exception {
    final String sqlQuery = "select t._type from " + TABLENAME + " t where t._type.something > 3";
    errorTypeTestHelper(sqlQuery, ErrorType.VALIDATION);
  }

  @Test
  public final void testFilterIndexItem() throws Exception {
    final String sqlQuery2 = "select t._index from " + TABLENAME + " t where t._index.something > 3";
    errorTypeTestHelper(sqlQuery2, ErrorType.VALIDATION);
  }

  @Test
  public final void testFilterUIDGreater() throws Exception {
    final String sqlQuery = "select _uid from " + TABLENAME + " where _uid > ' '";
    verifyJsonInPlanHelper(sqlQuery, new String[] {
        "=[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"_uid\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"},
      true);
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_uid")
      .baselineValues(uids[0])
      .baselineValues(uids[1])
      .baselineValues(uids[2])
      .baselineValues(uids[3])
      .baselineValues(uids[4])
      .go();
  }

  @Test
  public final void testFilterIDGreater() throws Exception {
    final String sqlQuery = "select _id from " + TABLENAME + " where _id > ' '";
    verifyJsonInPlanHelper(sqlQuery, new String[] {
        "=[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"_id\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"},
      true);
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_id")
      .baselineValues(ids[0])
      .baselineValues(ids[1])
      .baselineValues(ids[2])
      .baselineValues(ids[3])
      .baselineValues(ids[4])
      .go();
  }

  @Test
  public final void testFilterTypeGreater() throws Exception {
    final String sqlQuery = "select _type from " + TABLENAME + " where _type > ' '";
    verifyJsonInPlanHelper(sqlQuery, new String[]{
        "=[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"_type\" : {\n" +
        "        \"from\" : \" \",\n" +
        "        \"to\" : null,\n" +
        "        \"include_lower\" : false,\n" +
        "        \"include_upper\" : true\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"_type\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"},
      true);
    testBuilder().sqlQuery(sqlQuery).unOrdered()
      .baselineColumns("_type")
      .baselineValues(table)
      .baselineValues(table)
      .baselineValues(table)
      .baselineValues(table)
      .baselineValues(table)
      .go();
  }

  @Test
  public final void testFilterIndexGreater() throws Exception {
    final String sqlQuery2 = "select _index from " + TABLENAME + " where _index > ' '";
    verifyJsonInPlanHelper(sqlQuery2, new String[] {
        "=[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"_index\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"},
      true);
    testBuilder().sqlQuery(sqlQuery2).unOrdered()
      .baselineColumns("_index")
      .baselineValues(schema)
      .baselineValues(schema)
      .baselineValues(schema)
      .baselineValues(schema)
      .baselineValues(schema)
      .go();
  }

}
