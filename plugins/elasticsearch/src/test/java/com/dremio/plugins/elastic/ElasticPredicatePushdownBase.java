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
package com.dremio.plugins.elastic;

import static com.dremio.plugins.elastic.ElasticsearchCluster.PRIMITIVE_TYPES;
import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Random;

import org.apache.calcite.sql.SqlNode;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.NormalHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.plugins.elastic.planning.ElasticsearchGroupScan;
import com.dremio.sabot.rpc.user.UserSession;

public class ElasticPredicatePushdownBase extends ElasticBaseTestQuery {
  private static final Logger logger = LoggerFactory.getLogger(ElasticPredicatePushdownBase.class);

  private final Random random = new Random();

  private static QueryContext context;

  private ElasticVersionBehaviorProvider elasticVersionBehaviorProvider;
  protected static final String NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS = "float_field = integer_field";

  @BeforeClass
  public static void beforeClass() {
    context = new QueryContext(session(), getSabotContext(), QueryId.getDefaultInstance());
  }

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    elastic.populate(schema, table, 1);
    elasticVersionBehaviorProvider =  new ElasticVersionBehaviorProvider(elastic.getMinVersionInCluster());
  }


  // can not produce self-contradicting expression as they would be optimized out
  // WHERE a = 0 and a = 1 is turned into LIMIT 0
  // WHERE a = 0 and a = 0 is turned into WHERE a = 0
  protected String predicate(int n) {
    // to simplify we use each column only once
    n = Math.min(n, PRIMITIVE_TYPES.size() - 1);
    String[] predicates = new String[n];
    for (int i = 0; i < n; i++) {
      predicates[i] = primitiveTypePredicate(i);
    }

    String buffer = predicates[0];
    for (int i = 1; i < predicates.length; i++) {
      buffer = combine(buffer, predicates[i]);
    }

    return buffer;
  }

  /**
   * @param sql the SQL to plan for
   * @param fragment ES query to look for in push down
   * @throws Exception
   */
  protected void assertPushDownContains(String sql, String fragment) throws Exception {
    ElasticsearchGroupScan scan = generate(sql);
    String query = elasticVersionBehaviorProvider.processElasticSearchQuery(scan.getScanSpec().getQuery());
    compareJson(fragment, query);
  }


  private String combine(String predicate1, String predicate2) {
    return "(" + predicate1 + " " + binary[random.nextInt(1)] + " " + predicate2 + ")";
  }

  private String primitiveTypePredicate(int typeID) {
    ElasticsearchType[] array = new ElasticsearchType[PRIMITIVE_TYPES.size()];
    PRIMITIVE_TYPES.toArray(array);
    ElasticsearchType type = array[typeID];

    String value = randomValue(type);
    String field = fieldName(type);

    if (type == ElasticsearchType.TEXT) {
      value = "'" + value + "'";
    }

    return value + " " + operators[random.nextInt(operators.length)] + " " + field;
  }

  private String[] binary = {"AND", "OR"};
  private String[] operators = {"=", "<>", ">", "<"};

  private String fieldName(ElasticsearchType type) {
    switch (type) {
    case TEXT:
      return "text_field";
    case INTEGER:
      return "integer_field";
    case LONG:
      return "long_field";
    case FLOAT:
      return "float_field";
    case DOUBLE:
      return "double_field";
    case BOOLEAN:
      return "boolean_field";
    default:
      fail(format("Unexpected type in predicate test: %s", type));
      return null;
    }
  }

  private String randomValue(ElasticsearchType type) {
    switch (type) {
    case TEXT:
      StringBuilder randomAsciiBuilder = new StringBuilder();
      for (int i = 0; i < 11; i++) {
        randomAsciiBuilder.append((char)(random.nextInt(26) + 'a'));
      }
      return randomAsciiBuilder.toString();
    case INTEGER:
      return String.valueOf(random.nextInt());
    case LONG:
      return String.valueOf(random.nextLong());
    case FLOAT:
      return String.valueOf(random.nextFloat());
    case DOUBLE:
      return String.valueOf(random.nextDouble());
    case BOOLEAN:
      return String.valueOf(random.nextBoolean());
    default:
      fail(format("Unexpected type in predicate test: %s", type));
      return null;
    }
  }

  protected void validate(String predicate) throws Exception {

    ElasticsearchGroupScan scan = generateScanFromPredicate(predicate);
    assertTrue(scan.getScanSpec().isPushdown());

    String query = scan.getScanSpec().getQuery();
    assertNotNull(query);

    logger.debug("--> Generated query:\n{}", query);
  }

  private ElasticsearchGroupScan generateScanFromPredicate(String predicate) throws Exception {
    logger.debug("--> Testing predicate:\n{}", predicate);
    String sql = "select * from elasticsearch." + schema + "." + table + " where " + predicate;
    return generate(sql);
  }

  protected ElasticsearchGroupScan generate(String sql) throws Exception {
    AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));
    SqlConverter converter = new SqlConverter(context.getPlannerSettings(),
      context.getOperatorTable(), context, context.getMaterializationProvider(), context.getFunctionRegistry(),
      context.getSession(), observer, context.getSubstitutionProviderFactory(), context.getConfig(),
      context.getScanResult(), context.getRelMetadataQuerySupplier());
    SqlNode node = converter.parse(sql);
    SqlHandlerConfig config = new SqlHandlerConfig(context, converter, observer, null);
    NormalHandler handler = new NormalHandler();
    PhysicalPlan plan = handler.getPlan(config, sql, node);
    List<PhysicalOperator> operators = plan.getSortedOperators();
    ElasticsearchGroupScan scan = find(operators);
    assertNotNull("Physical plan does not contain an elasticsearch scan for query: " + sql, scan);
    return scan;
  }

  public static UserSession session() {
    return UserSession.Builder.newBuilder()
      .withSessionOptionManager(new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
        getSabotContext().getOptionManager())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(UserServiceTestImpl.TEST_USER_1).build())
      .setSupportComplexTypes(true)
      .build();
  }

  private ElasticsearchGroupScan find(List<PhysicalOperator> operators) {
    for (PhysicalOperator operator : operators) {
      if (operator instanceof ElasticsearchGroupScan) {
        return (ElasticsearchGroupScan) operator;
      }
    }
    return null;
  }
}
