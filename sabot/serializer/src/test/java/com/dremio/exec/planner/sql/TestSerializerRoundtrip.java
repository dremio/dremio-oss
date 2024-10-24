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
package com.dremio.exec.planner.sql;

import static com.dremio.exec.planner.rules.DremioCoreRules.GROUPSET_TO_CROSS_JOIN_RULE;
import static com.dremio.exec.planner.rules.DremioCoreRules.GROUP_SET_TO_CROSS_JOIN_RULE_ROLLUP;
import static com.dremio.exec.planner.rules.DremioCoreRules.REDUCE_FUNCTIONS_FOR_GROUP_SETS;
import static com.dremio.exec.planner.rules.DremioCoreRules.REWRITE_PROJECT_TO_FLATTEN_RULE;
import static com.dremio.exec.planner.sql.MockSchemas.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.ExecTest;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.DelegatingPlannerCatalog;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.DremioHepPlanner;
import com.dremio.exec.planner.MatchCountListener;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.serializer.ProtoRelSerializerFactory;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.resource.ClusterResourceInformation;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.DremioTest;
import com.dremio.test.GoldenFileTestBuilder;
import com.dremio.test.GoldenFileTestBuilder.Base64String;
import com.dremio.test.GoldenFileTestBuilder.MultiLineString;
import com.dremio.test.shams.ShamOptionResolver;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.poi.util.HexDump;
import org.junit.Assert;
import org.junit.Test;

/** Ensure serialization roundtrip produces valid plans. */
public class TestSerializerRoundtrip {
  public static final MockCatalog CATALOG =
      new MockCatalog(
          JavaTypeFactoryImpl.INSTANCE,
          ImmutableList.<MockDremioTable>builder()
              .add(createTable(EMP, "EMP"))
              .add(createTable(EMP, "EMP_MODIFIABLEVIEW"))
              .add(createTable(EMP, "EMP_MODIFIABLEVIEW2"))
              .add(createTable(EMP, "EMP_MODIFIABLEVIEW3"))
              .add(createTable(EMP, "EMPDEFAULTS"))
              .add(createTable(EMP, "EMP_20"))
              .add(createTable(DEPT, "DEPT"))
              .add(createTable(CUSTOMER, "SALES", "CUSTOMER"))
              .add(createTable(NATION, "SALES", "NATION"))
              .add(createTable(REGION, "SALES", "REGION"))
              .add(createTable(SALGRADE, "SALGRADE"))
              .add(createTable(TEST_SCHEMA, "T1"))
              .add(createTable(TEST_SCHEMA, "T2"))
              .add(createTable(TEST_SCHEMA, "T3"))
              .add(createTable(CUSTOMER, "cp", "tpch/customer.parquet"))
              .add(createTable(LINEITEM, "cp", "tpch/lineitem.parquet"))
              .add(createTable(NATION, "cp", "tpch/nation.parquet"))
              .add(createTable(ORDER, "cp", "tpch/orders.parquet"))
              .add(createTable(PART, "cp", "tpch/part.parquet"))
              .add(createTable(PART_SUPPLIER, "cp", "tpch/partsupp.parquet"))
              .add(createTable(REGION, "cp", "tpch/region.parquet"))
              .add(createTable(SUPPLIER, "cp", "tpch/supplier.parquet"))
              .build());
  public static final DremioCatalogReader CATALOG_READER =
      new DremioCatalogReader(DelegatingPlannerCatalog.newInstance(CATALOG));
  public static final ProtoRelSerializerFactory FACTORY =
      new ProtoRelSerializerFactory(ExecTest.CLASSPATH_SCAN_RESULT);
  public static final FunctionImplementationRegistry FUNCTIONS =
      FunctionImplementationRegistry.create(
          ExecTest.DEFAULT_SABOT_CONFIG, ExecTest.CLASSPATH_SCAN_RESULT);
  public static final SqlOperatorTable OPERATOR_TABLE =
      DremioCompositeSqlOperatorTable.create(FUNCTIONS);

  private static final ConcurrentMap<String, Object> fileSystemLocks = new ConcurrentHashMap<>();

  @Test
  public void testWindowSerde() {
    GoldenFileTestBuilder.<String, Output>create(
            TestSerializerRoundtrip::executeTestWithOpeartorExpansion)
        .add(
            "WINDOW function",
            "select sum(a) over(partition by b) as col1, count(*) over(partition by c) as col2 from t1")
        .add(
            "Calculate the Rank of Employees Based on Salary",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    DEPTNO, \n"
                + "    SAL, \n"
                + "    RANK() OVER (PARTITION BY DEPTNO ORDER BY SAL DESC) AS SALARY_RANK\n"
                + "FROM \n"
                + "    EMP\n")
        .add(
            "Calculate the Running Total of Salaries by Department",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    DEPTNO, \n"
                + "    SAL, \n"
                + "    SUM(SAL) OVER (PARTITION BY DEPTNO ORDER BY HIREDATE) AS RUNNING_TOTAL_SAL\n"
                + "FROM \n"
                + "    EMP\n")
        .add(
            "Find the Highest Salary in Each Department",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    DEPTNO, \n"
                + "    SAL, \n"
                + "    MAX(SAL) OVER (PARTITION BY DEPTNO) AS MAX_SALARY_IN_DEPT\n"
                + "FROM \n"
                + "    EMP\n")
        .add(
            "Calculate the Difference Between an Employee's Salary and the Average Salary of Their Department",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    DEPTNO, \n"
                + "    SAL, \n"
                + "    AVG(SAL) OVER (PARTITION BY DEPTNO) AS AVG_DEPT_SAL,\n"
                + "    SAL - AVG(SAL) OVER (PARTITION BY DEPTNO) AS SALARY_DIFF_FROM_AVG\n"
                + "FROM \n"
                + "    EMP\n")
        .add(
            "Find the Previous and Next Employee's Salary",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    SAL, \n"
                + "    LAG(SAL) OVER (ORDER BY HIREDATE) AS PREV_SALARY,\n"
                + "    LEAD(SAL) OVER (ORDER BY HIREDATE) AS NEXT_SALARY\n"
                + "FROM \n"
                + "    EMP\n")
        .add(
            "Calculate the Percent Rank of Each Employee's Salary Within the Company",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    SAL, \n"
                + "    PERCENT_RANK() OVER (ORDER BY SAL DESC) AS SALARY_PERCENT_RANK\n"
                + "FROM \n"
                + "    EMP\n")
        .add(
            "Cumulative Distribution of Employees' Salaries",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    SAL, \n"
                + "    CUME_DIST() OVER (ORDER BY SAL DESC) AS SALARY_CUME_DIST\n"
                + "FROM \n"
                + "    EMP\n")
        .add(
            "Moving Average of Salaries Over the Last 3 Employees by Hire Date",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    SAL, \n"
                + "    AVG(SAL) OVER (ORDER BY HIREDATE ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MOVING_AVG_SAL\n"
                + "FROM \n"
                + "    EMP\n")
        .add(
            "Cumulative Salary by Department Ordered by Hire Date",
            "SELECT \n"
                + "    EMPNO, \n"
                + "    ENAME, \n"
                + "    DEPTNO, \n"
                + "    SAL, \n"
                + "    HIREDATE,\n"
                + "    SUM(SAL) OVER (PARTITION BY DEPTNO ORDER BY HIREDATE ASC) AS CUMULATIVE_SALARY\n"
                + "FROM \n"
                + "    EMP")
        .runTests();
  }

  @Test
  public void testQueries() {
    GoldenFileTestBuilder.<String, Output>create(TestSerializerRoundtrip::executeTest)
        .add("SELECT", "select a,b,c from t1")
        .add("SELECT AS", "select T.a as A, T.b as B, T.c as C from t1 as T")
        .add("SELECT DISTINCT", "select distinct a from t1 where b > 10")
        .add("SELECT EXPRESSION", "select a+b as A, c from t1")
        .add("FILTER", "select a, b, c from t1 where a > 10")
        .add("FILTER NOT EQUALS", "select a, b, c from t1 where a <> 10")
        .add("FILTER NOT NULL", "select a, b, c from t1 where a IS NOT NULL")
        .add("FILTER IS NULL", "select a, b, c from t1 where a IS NULL")
        .add(
            "FILTER COMPOUND",
            "select a, b, c from t1 where ((a > 10 AND (c+1 = 3 OR a < 100)) OR b =3)")
        .add(
            "SUBQUERY",
            "select sub.A, sub.B, sub.C from (SELECT T.a A, T.b B, T.c C FROM t1 T where T.b > 3 order by c desc) sub")
        .add("LIMIT", "select a, b, c from t1 limit 10")
        .add("OFFSET", "select a, b, c from t1 order by a limit 5 offset 10 rows")
        .add(
            "LIMIT OFFSET FETCH",
            "select a, b, c from t1 order by a limit 10 offset 10 fetch next 3 rows only")
        .add(
            "VALUES",
            "select t1.id as id from (VALUES 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1) AS t1(id)")
        .add("FILTER CAST INTEGER", "select a, b, c from t1 where a > CAST(3.14159 AS INTEGER)")
        .add("FILTER CAST DATE", "select a, b, c from t1 where d > CAST('2019-01-01' as DATE)")
        .add("FILTER CAST DATE 2", "select a, b, c from t1 where d > CAST('2019:01:01' as DATE)")
        .add("AGGREGATE", "select a, count(b) from t1 group by a")
        .add("MULTIPLE AGGREGATES", "select a, count(b), sum(b), avg(b) from t1 group by a, c")
        .add("AGGREGATE WITH ORDER BY", "select a, count(b) from t1 group by a order by a asc")
        .add("JOIN", "select t1.a, t1.b, t2.c, t2.d from t1 join t2 on t1.id = t2.id")
        .add("LEFT JOIN", "select t1.a, t1.b, t2.c, t2.d from t1 join t2 on t1.id = t2.id")
        .add("RIGHT JOIN", "select t1.a, t1.b, t2.c, t2.d from t1 right join t2 on t1.id = t2.id")
        .add("FULL JOIN", "select t1.a, t1.b, t2.c, t2.d from t1 full join t2 on t1.id = t2.id")
        .add(
            "WINDOW",
            "select sum(a) over(partition by b) as col1, count(*) over(partition by c) as col2 from t1")
        .add(
            "UNION",
            "SELECT a as A FROM t1 union SELECT a as A FROM t2 union SELECT a as A FROM t3")
        .add(
            "CORRELATED EXISTS",
            "select a, b, c from t1 l where exists (select * from t2 r where r.a = l.a AND l.b=1.1)")
        .add(
            "CORRELATED NOT EXISTS",
            "select a, b, c from t1 l where not exists (select * from t2 r where r.a = l.a AND l.b=1.1)")
        .add("IN", "SELECT a, b,c  FROM t1 l WHERE l.b IN (SELECT r.a FROM t2 r WHERE r.a > 3)")
        .add("INTERSECT", "select a,b,c from t1 intersect select a,b,c from t1")
        .add("EXCEPT", "select a,b,c from t1 except select a,b,c from t1")
        .runTests();
  }

  @Test
  public void testColumnAliases() {
    GoldenFileTestBuilder.<String, Output>create(TestSerializerRoundtrip::executeTest)
        .add(
            "SELECT",
            "SELECT ename as employee_name, LOWER(employee_name) as lower_employee_name FROM emp")
        // This fails since the alias extender only does one layer of unaliasing
        // .add("Double aliasing", "SELECT ename as employee_name, LOWER(employee_name) as
        // lower_employee_name, UPPER(lower_employee_name) as upper_employee_name FROM emp")
        .add(
            "WHERE",
            "SELECT LOWER(ename) as lower_employee_name FROM emp WHERE lower_employee_name ='bob'")
        .add(
            "GROUP BY + HAVING",
            "SELECT MAX(deptno), LOWER(ename) as emp_name FROM emp GROUP BY emp_name HAVING emp_name = 'john'")
        .add("ORDER BY", "SELECT LOWER(ename) as emp_name FROM emp ORDER BY emp_name")
        .runTests();
  }

  @Test
  public void testQualify() {
    GoldenFileTestBuilder.<String, Output>create(TestSerializerRoundtrip::executeTest)
        .add(
            "QUALIFY WITHOUT REFERENCES",
            "SELECT empno, ename, deptno FROM emp QUALIFY ROW_NUMBER() over (partition by ename order by deptno) = 1")
        .add(
            "QUALIFY WITHOUT REFERENCES AND FILTER",
            "SELECT empno, ename, deptno FROM emp WHERE deptno > 5 QUALIFY ROW_NUMBER() over (partition by ename order by deptno) = 1")
        .add(
            "QUALIFY WITH REFERENCES",
            "SELECT empno, ename, deptno, ROW_NUMBER() over (partition by ename order by deptno) as row_num FROM emp QUALIFY row_num = 1")
        .add(
            "QUALIFY WITH DERIVED COLUMN",
            "SELECT empno, ename, deptno, SUBSTRING(ename, 1, 1) as derived_column FROM emp QUALIFY ROW_NUMBER() over (partition by derived_column order by deptno) = 1")
        .add(
            "QUALIFY WITH WINDOW CLAUSE",
            "SELECT empno, ename, deptno, ROW_NUMBER() over myWindow as row_num FROM emp WINDOW myWindow AS (PARTITION BY ename ORDER BY deptno) QUALIFY row_num = 1")
        /* There is no RelSerde for LogicalTableModify.
        .add("QUALIFY IN DDL", "INSERT INTO dept(deptno, name) "
          + "SELECT DISTINCT empno, ename "
          + "FROM emp "
          + "WHERE deptno > 5 "
          + "QUALIFY RANK() OVER (PARTITION BY ename ORDER BY slacker DESC) = 1 ")
         */
        .add(
            "QUALIFY IN SUBQUERY",
            "SELECT * "
                + "FROM ("
                + " SELECT DISTINCT empno, ename, deptno "
                + " FROM emp "
                + " QUALIFY RANK() OVER (PARTITION BY ename ORDER BY deptno DESC) = 1 )")
        .runTests();
  }

  @Test
  public void testSqlFunction() {
    GoldenFileTestBuilder.<String, Output>create(TestSerializerRoundtrip::executeTest)
        .add("ROUND", "SELECT ROUND(CAST(9.9 AS DECIMAL(2,1)))FROM (VALUES (1)) AS t(a)")
        .add("TRUNCATE", "SELECT TRUNCATE(CAST(9.9 AS DECIMAL(2,1))) FROM (VALUES (1)) AS t(a)")
        .add("MEDAIN", "SELECT MEDIAN(A) OVER (PARTITION BY b) FROM (VALUES(1, 2)) AS t(a, b)")
        .add("CONCAT FUNCTION", "SELECT CONCAT(a, b) FROM (VALUES('hello', 'world')) AS t(a, b)")
        .add("CONCAT OPERATOR", "SELECT a || b FROM (VALUES('hello', 'world')) AS t(a, b)")
        .add(
            "PERCENTILE_CONT",
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b)\n"
                + "FROM (VALUES (1, 2)) AS t(a, b)")
        .runTests();
  }

  @Test
  public void testSqlToRelConvertTests() {
    GoldenFileTestBuilder.<String, Output>create(TestSerializerRoundtrip::executeTest)
        .allowExceptions()
        .add("Integer Literal", "select 1 from emp")
        .add(
            "Interval Literal Year To Month",
            "select   cast(empno as Integer) * (INTERVAL '1-1' YEAR TO MONTH) from emp")
        .add(
            "Interval Literal Hour To Minute",
            "select  cast(empno as Integer) * (INTERVAL '1:1' HOUR TO MINUTE) from emp")
        .add("Select Distinct", "select distinct sal + 5 from emp")
        .add("Select Distinct Group", "select distinct sum(sal) from emp group by deptno")
        .add("Explicit Table", "table emp")
        .add("In Value List Short", "select empno from emp where deptno in (10, 20)")
        .add(
            "In Value List Long",
            "select empno from emp where deptno in (10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230)")
        .add(
            "In Uncorrelated Sub Query",
            "select empno from emp where deptno in (select deptno from dept)")
        .add("Order", "select empno from emp order by empno, empno desc")
        .add("Order Desc Nulls Last", "select empno from emp order by empno desc nulls last")
        .add("Order Based Repeat Fields", "select empno from emp order by empno DESC, empno ASC")
        .add("Order By Alias", "select empno + 1 as x, empno - 2 as y from emp order by y")
        .add("Order By Ordinal Desc", "select empno + 1, deptno, empno from emp order by 2.5 desc")
        .add(
            "Order By Identical Expr",
            "select empno + 1 from emp order by deptno asc, empno + 1 desc")
        .add(
            "Order By Negative Ordinal",
            "select empno + 1, deptno, empno from emp order by -1 desc")
        .add(
            "Order By Ordinal In Expr",
            "select empno + 1, deptno, empno from emp order by 1 + 2 desc")
        .add(
            "Order By Alias Does Not Obscure",
            "select empno + 1 as empno, empno - 2 as y from emp order by empno + 3")
        .add(
            "Order By Alias In Expr",
            "select empno + 1 as x, empno - 2 as y from emp order by y + 3")
        .add(
            "Order By Alias Overrides",
            "select empno + 1 as empno, empno - 2 as y from emp order by empno + 3")
        .add(
            "Order Union",
            "select empno, sal from emp union all select deptno, deptno from dept order by sal desc, empno asc")
        .add(
            "Order Union Exprs",
            "select empno, sal from emp union all select deptno, deptno from dept order by empno * sal + 2")
        .add(
            "Order Group",
            "select deptno, count(*) from emp group by deptno order by deptno * sum(sal) desc, min(empno)")
        .add(
            "Order Distinct",
            "select distinct empno, deptno + 1 from emp order by deptno + 1 + empno")
        .add(
            "Order Union Ordinal",
            "select empno, sal from emp union all select deptno, deptno from dept order by 2")
        .add("Sample", "select * from emp tablesample substitute('DATASET1') where empno > 5")
        .add("Over Order Window", "select last_value(deptno) over (order by empno) from emp ")
        /* DX-36624
        .add(
          "Over Order Following Window",
          "select   last_value(deptno) over (order by empno rows 2 following) from emp ") */
        .add("Interval", "values(cast(interval '1' hour as interval hour to second))")
        /* DX-36624
        .add(
          "Over Avg2",
          "select sum(sal) over w1,   avg(CAST(sal as real)) over w1 from emp window w1 as (partition by job order by hiredate rows 2 preceding)")*/
        .add("Join Using", "SELECT * FROM emp JOIN dept USING (deptno)")
        .add(
            "Join Using Compound",
            "SELECT * FROM emp LEFT JOIN (SELECT *, deptno * 5 as empno FROM dept) USING (deptno,empno)")
        .add("Join On", "SELECT * FROM emp JOIN dept on emp.deptno = dept.deptno")
        .add(
            "Join On In",
            "select * from emp join dept  on emp.deptno = dept.deptno and emp.empno in (1, 3)")
        .add("Sample Bernoulli", "select * from emp tablesample bernoulli(50) where empno > 5")
        /* Column Ambiguous
        .add(
          "Sample Bernoulli Query",
          "select * from (  select * from emp as e tablesample bernoulli(10) repeatable(1)  join dept on e.deptno = dept.deptno ) tablesample bernoulli(50) repeatable(99) where empno > 5") */
        .add("Sample System", "select * from emp tablesample system(50) where empno > 5")
        /* Column Ambiguous
        .add(
          "Sample System Query",
          "select * from (  select * from emp as e tablesample system(10) repeatable(1)  join dept on e.deptno = dept.deptno ) tablesample system(50) repeatable(99) where empno > 5")*/
        .add(
            "Select Distinct Dup",
            "select distinct sal + 5, deptno, sal + 5 from emp where deptno < 10")
        .add("Count No Group", "select count(*), sum(sal) from emp where empno > 10")
        .add(
            "Alias List",
            "select a + b from (   select deptno, 1 as uno, name from dept ) as d(a, b, c) where c like 'X%'")
        /* Column Ambiguous
        .add(
          "Alias List2",
          "select * from (   select a, b, c from (values (1, 2, 3)) as t (c, b, a) ) join dept on dept.deptno = c order by c + a")*/
        .add("Struct Type Alias", "select t.r AS myRow from (select row(row(1)) r from dept) t")
        .add(
            "Join Using Dynamic Table",
            "select * from SALES.NATION t1 join SALES.NATION t2 using (n_nationkey)")
        .add(
            "Join With Union",
            "select grade from (select empno from emp union select deptno from dept),   salgrade")
        .add("Join Natural", "SELECT * FROM emp NATURAL JOIN dept")
        .add(
            "Join Natural No Common Column",
            "SELECT * FROM emp NATURAL JOIN (SELECT deptno AS foo, name FROM dept) AS d")
        .add(
            "Join Natural Multiple Common Column",
            "SELECT * FROM emp NATURAL JOIN (SELECT deptno, name AS ename FROM dept) AS d")
        .add(
            "Order Offset Fetch",
            "select empno from emp order by empno offset 10 rows fetch next 5 rows only")
        .add(
            "Order Offset Fetch With Dynamic Parameter",
            "select empno from emp order by empno offset ? rows fetch next ? rows only")
        .add("Fetch", "select empno from emp fetch next 5 rows only")
        .add("Fetch With Dynamic Parameter", "select empno from emp fetch next ? rows only")
        .add("Offset Fetch", "select empno from emp offset 10 rows fetch next 5 rows only")
        .add(
            "Offset Fetch With Dynamic Parameter",
            "select empno from emp offset ? rows fetch next ? rows only")
        .add("Offset", "select empno from emp offset 10 rows")
        .add("Offset With Dynamic Parameter", "select empno from emp offset ? rows")
        .add(
            "Join Using Dynamic Table",
            "select * from SALES.NATION t1 join SALES.NATION t2 using (n_nationkey)")
        .add(
            "Multi And",
            "select * from emp where deptno < 10 and deptno > 5 and (deptno = 8 or empno < 100)")
        .add(
            "Join Using Three Way",
            "select * from emp as e join dept as d using (deptno) join emp as e2 using (empno)")
        .add(
            "Join On Expression", "SELECT * FROM emp JOIN dept on emp.deptno + 1 = dept.deptno - 2")
        .add("With", "with emp2 as (select * from emp) select * from emp2")
        .add(
            "With Union",
            "with emp2 as (select * from emp where deptno > 10) select empno from emp2 where deptno < 30 union all select deptno from emp")
        .add(
            "With Inside Where Exists",
            "select * from emp where exists (   with dept2 as (select * from dept where dept.deptno >= emp.deptno)   select 1 from dept2 where deptno <= emp.deptno)")
        .add(
            "With Inside Scalar Sub Query",
            "select (  with dept2 as (select * from dept where deptno > 10) select count(*) from dept2) as c from emp")
        .add("Aggregate No Group", "select sum(deptno) from emp")
        .add(
            "Condition Off By One Reversed",
            "SELECT * FROM emp JOIN dept on dept.deptno = emp.deptno + 0")
        .add("Condition Off By One", "SELECT * FROM emp JOIN dept on emp.deptno + 0 = dept.deptno")
        .add(
            "Lateral Decorrelate",
            "select * from emp,  LATERAL (select * from dept where emp.deptno=dept.deptno)")
        .add(
            "Nested Correlations",
            "select * from (select 2+deptno d2, 3+deptno d3 from emp) e  where exists (select 1 from (select deptno+1 d1 from dept) d  where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)  where d4=d.d1 and d5=d.d1 and d6=e.d3))")
        .add(
            "Nested Correlations Decorrelated",
            "select * from (select 2+deptno d2, 3+deptno d3 from emp) e  where exists (select 1 from (select deptno+1 d1 from dept) d  where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)  where d4=d.d1 and d5=d.d1 and d6=e.d3))")
        .add(
            "With Inside Where Exists Decorrelate",
            "select * from emp where exists (   with dept2 as (select * from dept where dept.deptno >= emp.deptno)   select 1 from dept2 where deptno <= emp.deptno)")
        .add(
            "Exists Correlated Decorrelate",
            "select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno)")
        .add(
            "Exists Correlated Limit",
            "select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno limit 1)")
        .add(
            "Exists Correlated Limit Decorrelate",
            "select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno limit 1)")
        .add("Join Unnest", "select*from dept as d, unnest(multiset[d.deptno * 2])")
        .add(
            "Nested Aggregates",
            "SELECT   avg(sum(sal) + 2 * min(empno) + 3 * avg(empno))   over (partition by deptno) from emp group by deptno")
        .add("With Order", "with emp2 as (select * from emp) select * from emp2 order by deptno")
        .add(
            "With Union Order",
            "with emp2 as (select empno, deptno as x from emp) select * from emp2 union all select * from emp2 order by empno + x")
        .add(
            "In Uncorrelated Sub Query In Select",
            "select name, deptno in (   select case when true then deptno else null end from emp) from dept")
        .add(
            "Not In Uncorrelated Sub Query In Select",
            "select empno, deptno not in (   select case when true then deptno else null end from dept) from emp")
        .add(
            "Not In Uncorrelated Sub Query",
            "select empno from emp where deptno not in (select deptno from dept)")
        .add(
            "Not In Uncorrelated Sub Query In Select Not Null",
            "select empno, deptno not in (   select deptno from dept) from emp")
        .add("Sort With Trim", "select ename from (select * from emp order by sal) a")
        .add(
            "Group Alias",
            "select \"$f2\", max(x), max(x + 1) from (values (1, 2)) as t(\"$f2\", x) group by \"$f2\"")
        .add(
            "Not In Uncorrelated Sub Query In Select Deduce Not Null",
            "select empno, deptno not in (   select mgr from emp where mgr > 5) from emp")
        .add(
            "Not In Uncorrelated Sub Query In Select Deduce Not Null2",
            "select empno, deptno not in (   select mgr from emp where mgr is not null) from emp")
        .add(
            "Not In Uncorrelated Sub Query In Select Deduce Not Null3",
            "select empno, deptno not in (   select mgr from emp where mgr in (     select mgr from emp where deptno = 10)) from emp")
        .add(
            "Not In Uncorrelated Sub Query In Select May Be Null",
            "select empno, deptno not in (   select mgr from emp) from emp")
        .add("Singleton Grouping Set", "select sum(sal) from emp group by grouping sets (deptno)")
        .add("Group Empty", "select sum(deptno) from emp group by ()")
        .add("Group By With Duplicates", "select sum(sal) from emp group by (), ()")
        .add(
            "Grouping Sets",
            "select deptno, ename, sum(sal) from emp group by grouping sets ((deptno), (ename, deptno)) order by 2")
        .add(
            "Grouping Sets With Rollup",
            "select deptno, ename, sum(sal) from emp group by grouping sets ( rollup(deptno), (ename, deptno)) order by 2")
        .add(
            "Grouping Sets With Cube",
            "select deptno, ename, sum(sal) from emp group by grouping sets ( (deptno), CUBE(ename, deptno)) order by 2")
        .add(
            "Grouping Sets With Rollup Cube",
            "select deptno, ename, sum(sal) from emp group by grouping sets ( CUBE(deptno), ROLLUP(ename, deptno)) order by 2")
        .add(
            "Duplicate Grouping Sets",
            "select sum(sal) from emp group by sal,   grouping sets (deptno,     grouping sets ((deptno, ename), ename),       (ename)),   ()")
        .add(
            "Grouping Sets Cartesian Product",
            "select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by grouping sets (a, b), grouping sets (c, d)")
        .add(
            "Grouping Sets Cartesian Product2",
            "select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by grouping sets (a, (a, b)), grouping sets (c), d")
        .add(
            "Rollup",
            "select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by rollup(a, b), rollup(c, d)")
        .add("Cube", "select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by cube(a, b)")
        .add(
            "Rollup Tuples",
            "select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by rollup(b, (a, d))")
        .add(
            "Grouping Sets With",
            "with t(a, b, c, d) as (values (1, 2, 3, 4)) select 1 from t group by rollup(a, b), rollup(c, d)")
        .add(
            "Rollup Simple",
            "select a, b, count(*) as c from (values (cast(null as integer), 2)) as t(a, b) group by rollup(a, b)")
        .add(
            "Group By Expression",
            "select count(*) from emp group by substring(ename FROM 1 FOR 1)")
        .add(
            "Grouping Sets Product",
            "select 1 from (values (0, 1, 2, 3, 4)) as t(a, b, c, x, y) group by grouping sets ((a, b), c), grouping sets ((x, y), ())")
        .add(
            "Grouping Function With Group By",
            "select   deptno, grouping(deptno), count(*), grouping(empno) from emp group by empno, deptno order by 2")
        .add(
            "Grouping Function",
            "select   deptno, grouping(deptno), count(*), grouping(empno) from emp group by rollup(empno, deptno)")
        .add(
            "Match Recognize1",
            "select *   from emp match_recognize   (     partition by job, sal     order by job asc, sal desc, empno     pattern (strt down+ up+)     define       down as down.mgr < PREV(down.mgr),       up as up.mgr > prev(up.mgr)) as mr")
        .add(
            "Match Recognize Measures1",
            "select * from emp match_recognize (   partition by job, sal   order by job asc, sal desc   measures MATCH_NUMBER() as match_num,     CLASSIFIER() as var_match,     STRT.mgr as start_nw,     LAST(DOWN.mgr) as bottom_nw,     LAST(up.mgr) as end_nw   pattern (strt down+ up+)   define     down as down.mgr < PREV(down.mgr),     up as up.mgr > prev(up.mgr)) as mr")
        .add(
            "Match Recognize Measures2",
            "select * from emp match_recognize (   partition by job   order by sal   measures MATCH_NUMBER() as match_num,     CLASSIFIER() as var_match,     STRT.mgr as start_nw,     LAST(DOWN.mgr) as bottom_nw,     LAST(up.mgr) as end_nw   pattern (strt down+ up+)   define     down as down.mgr < PREV(down.mgr),     up as up.mgr > prev(up.mgr)) as mr")
        .add(
            "Match Recognize Measures3",
            "select * from emp match_recognize (   partition by job   order by sal   measures MATCH_NUMBER() as match_num,     CLASSIFIER() as var_match,     STRT.mgr as start_nw,     LAST(DOWN.mgr) as bottom_nw,     LAST(up.mgr) as end_nw   ALL ROWS PER MATCH   pattern (strt down+ up+)   define     down as down.mgr < PREV(down.mgr),     up as up.mgr > prev(up.mgr)) as mr")
        .add(
            "Match Recognize Pattern Skip1",
            "select *   from emp match_recognize   (     after match skip to next row     pattern (strt down+ up+)     define       down as down.mgr < PREV(down.mgr),       up as up.mgr > NEXT(up.mgr)   ) mr")
        .add(
            "Match Recognize Prev Down",
            "SELECT * FROM emp MATCH_RECOGNIZE (   MEASURES     STRT.mgr AS start_mgr,     LAST(DOWN.mgr) AS up_days,     LAST(UP.mgr) AS total_days   PATTERN (STRT DOWN+ UP+)   DEFINE     DOWN AS DOWN.mgr < PREV(DOWN.mgr),     UP AS UP.mgr > PREV(DOWN.mgr) ) AS T")
        .add(
            "Match Recognize Prev Last",
            "SELECT * FROM emp MATCH_RECOGNIZE (   MEASURES     STRT.mgr AS start_mgr,     LAST(DOWN.mgr) AS bottom_mgr,     LAST(UP.mgr) AS end_mgr   ONE ROW PER MATCH   PATTERN (STRT DOWN+ UP+)   DEFINE     DOWN AS DOWN.mgr < PREV(DOWN.mgr),     UP AS UP.mgr > PREV(LAST(DOWN.mgr, 1), 1) ) AS T")
        .add(
            "Match Recognize Subset1",
            "select *   from emp match_recognize   (     after match skip to down     pattern (strt down+ up+)     subset stdn = (strt, down)     define       down as down.mgr < PREV(down.mgr),       up as up.mgr > NEXT(up.mgr)   ) mr")
        .add(
            "Prev Classifier",
            "SELECT * FROM emp MATCH_RECOGNIZE (   MEASURES     STRT.mgr AS start_mgr,     LAST(DOWN.mgr) AS up_days,     LAST(UP.mgr) AS total_days   PATTERN (STRT DOWN? UP+)   DEFINE     DOWN AS DOWN.mgr < PREV(DOWN.mgr),     UP AS CASE             WHEN PREV(CLASSIFIER()) = 'STRT'               THEN UP.mgr > 15             ELSE               UP.mgr > 20             END ) AS T")
        .add("Not Not In", "select * from EMP where not (ename not in ('Fred') )")
        .add("Table Subset", "select deptno, name from dept")
        .add("Table Expression", "select deptno + deptno from dept")
        /* Unsupported operation (Correlated sub-queries in ON clauses are not supported)
        .add(
          "Table Extend",
          "select * from dept extend (x varchar(5) not null)")
        .add(
          "Table Extend Subset",
          "select deptno, x from dept extend (x int)")
        .add(
          "Table Extend Expression",
          "select deptno + x from dept extend (x int not null)")
        .add(
          "Modifiable View Extend",
          "select * from EMP_MODIFIABLEVIEW extend (x varchar(5) not null)")
        .add(
          "Modifiable View Extend Subset",
          "select x, empno from EMP_MODIFIABLEVIEW extend (x varchar(5) not null)")
        .add(
          "Modifiable View Extend Expression",
          "select empno + x from EMP_MODIFIABLEVIEW extend (x int not null)")*/
        .add(
            "Select Modifiable View Constraint",
            "select deptno from EMP_MODIFIABLEVIEW2 where deptno = ?")
        .add("Modifiable View Ddl Extend", "select extra from EMP_MODIFIABLEVIEW2")
        .add(
            "Group By Case Sub Query",
            "SELECT CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END FROM emp GROUP BY (CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END)")
        .add(
            "Agg No Duplicate Column Names",
            "SELECT  empno, EXPR$2, COUNT(empno) FROM (     SELECT empno, deptno AS EXPR$2     FROM emp) GROUP BY empno, EXPR$2")
        .add("Agg Case Sub Query", "SELECT SUM(CASE WHEN empno IN (3) THEN 0 ELSE 1 END) FROM emp")
        .add(
            "Agg Case In Sub Query",
            "SELECT SUM(   CASE WHEN deptno IN (SELECT deptno FROM dept) THEN 1 ELSE 0 END) FROM emp")
        .add("Agg Scalar Sub Query", "SELECT SUM(SELECT min(deptno) FROM dept) FROM emp")
        .add(
            "Not In With Literal",
            "SELECT * FROM SALES.NATION WHERE n_name NOT IN     (SELECT ''      FROM SALES.NATION)")
        .add(
            "Group By Case In",
            "select  (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END),  min(empno) from EMP group by (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END)")
        .add(
            "Sub Query Aggregate Function Followed By Simple Operation",
            "select deptno from EMP where deptno > (select min(deptno) * 2 + 10 from EMP)")
        .add("Sub Query Values", "select deptno from EMP where deptno > (values 10)")
        .add(
            "Sub Query Limit One",
            "select deptno from EMP where deptno > (select deptno from EMP order by deptno limit 1)")
        .add(
            "Identical Expression In Sub Query",
            "select deptno from EMP where deptno in (1, 2) or deptno in (1, 2)")
        .add(
            "Having Aggr Function In",
            "select deptno from emp group by deptno having sum(case when deptno in (1, 2) then 0 else 1 end) + sum(case when deptno in (3, 4) then 0 else 1 end) > 10")
        .add(
            "Having In Sub Query With Aggr Function",
            "select sal from emp group by sal having sal in (   select deptno   from dept   group by deptno   having sum(deptno) > 0)")
        .add(
            "Aggregate And Scalar Sub Query In Having",
            "select deptno from emp group by deptno having max(emp.empno) > (SELECT min(emp.empno) FROM emp) ")
        .add(
            "Aggregate And Scalar Sub Query In Select",
            "select deptno,   max(emp.empno) > (SELECT min(emp.empno) FROM emp) as b from emp group by deptno ")
        /* Casting issue
        .add(
          "Insert",
          "insert into empnullables (deptno, empno, ename) values (10, 150, 'Fred')")*/
        /* Failure parsing the query
        .add(
          "Insert Extended Charset",
          "insert into empdefaults(updated TIMESTAMP)  (ename, deptno, empno, updated, sal)  values ('Freddie', 567, 40, 2017-03-12 13:03:05, 999999 }, {'上海', 456, 44, 2017-03-12 13:03:05, 999999)")*/
        .add("Select Extended Charset", "select * from empdefaults where ename = '上海'")
        /* Casting issue
        .add(
          "Insert Subset",
          "insert into empnullables values (50, 'Fred')")
        .add(
          "Insert Bind",
          "insert into empnullables (deptno, empno, ename) values (?, ?, ?)")
        .add(
          "Insert Bind Subset",
          "insert into empnullables values (?, ?)")
        .add(
          "Insert Bind Extended Column",
          "insert into empdefaults(updated TIMESTAMP) (ename, deptno, empno, updated, sal) values ('Fred', 456, 44, ?, 999999)")
        .add(
          "Insert Extended Column Modifiable View",
          "insert into EMP_MODIFIABLEVIEW2(updated TIMESTAMP) (ename, deptno, empno, updated, sal) values ('Fred', 20, 44, timestamp '2017-03-12 13:03:05', 999999)")
        .add(
          "Insert Bind Extended Column Modifiable View",
          "insert into EMP_MODIFIABLEVIEW2(updated TIMESTAMP) (ename, deptno, empno, updated, sal) values ('Fred', 20, 44, ?, 999999)")*/
        .add("Select View", "select * from emp_20 where empno > 100")
        /* Not supported
        .add(
          "Select View Extended Column Collision",
          "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR  from EMP_MODIFIABLEVIEW3 extend (SAL int)  where SAL = 20")
        .add(
          "Select View Extended Column Case Sensitive Collision",
          "select ENAME, EMPNO, JOB, SLACKER, \"sal\", HIREDATE, MGR  from EMP_MODIFIABLEVIEW3 extend (\"sal\" boolean)  where \"sal\" = true")
        .add(
          "Select View Extended Column Extended Collision",
          "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, EXTRA  from EMP_MODIFIABLEVIEW2 extend (EXTRA boolean)  where SAL = 20")
        .add(
          "Select View Extended Column Case Sensitive Extended Collision",
          "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, \"extra\"  from EMP_MODIFIABLEVIEW2 extend (\"extra\" boolean)  where \"extra\" = false")
        .add(
          "Select View Extended Column Underlying Collision",
          "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, COMM  from EMP_MODIFIABLEVIEW3 extend (COMM int)  where SAL = 20")
        .add(
          "Select View Extended Column Case Sensitive Underlying Collision",
          "select ENAME, EMPNO, JOB, SLACKER, SAL, HIREDATE, MGR, \"comm\"  from EMP_MODIFIABLEVIEW3 extend (\"comm\" int)  where \"comm\" = 20")*/
        /* Casting Issue
        .add(
          "Insert View",
          "insert into empnullables_20 (empno, ename) values (150, 'Fred')")
        .add(
          "Insert Subset View",
          "insert into empnullables_20 values (10, 'Fred')")
        .add(
          "Insert Modifiable View",
          "insert into EMP_MODIFIABLEVIEW (EMPNO, ENAME, JOB) values (34625, 'nom', 'accountant')")
        .add(
          "Insert Subset Modifiable View",
          "insert into EMP_MODIFIABLEVIEW values (10, 'Fred')")
        .add(
          "Insert Bind Modifiable View",
          "insert into EMP_MODIFIABLEVIEW (empno, job) values (?, ?)")
        .add(
          "Insert Bind Subset Modifiable View",
          "insert into EMP_MODIFIABLEVIEW values (?, ?)")
        .add(
          "Insert With Custom Initializer Expression Factory",
          "insert into empdefaults (deptno) values (300)")
        .add(
          "Insert Subset With Custom Initializer Expression Factory",
          "insert into empdefaults values (100)")
        .add(
          "Insert Bind With Custom Initializer Expression Factory",
          "insert into empdefaults (deptno) values (?)")
        .add(
          "Insert Bind Subset With Custom Initializer Expression Factory",
          "insert into empdefaults values (?)")
        .add(
          "Insert With Custom Column Resolving",
          "insert into struct.t values (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .add(
          "Insert With Custom Column Resolving2",
          "insert into struct.t_nullables (f0.c0, f1.c2, c1) values (?, ?, ?)")
        .add(
          "Insert View With Custom Column Resolving",
          "insert into struct.t_10 (f0.c0, f1.c2, c1, k0,   f1.a0, f2.a0, f0.c1, f2.c3) values (?, ?, ?, ?, ?, ?, ?, ?)")
        .add(
          "Update With Custom Column Resolving",
          "update struct.t set c0 = c0 + 1")
        .add(
          "Custom Column Resolving",
          "select k0 from struct.t")
        .add(
          "Custom Column Resolving2",
          "select c2 from struct.t")
        .add(
          "Custom Column Resolving3",
          "select f1.c2 from struct.t")
        .add(
          "Custom Column Resolving4",
          "select c1 from struct.t order by f0.c1")
        .add(
          "Custom Column Resolving5",
          "select count(c1) from struct.t group by f0.c1")
        .add(
          "Custom Column Resolving With Select Star",
          "select * from struct.t")
        .add(
          "Custom Column Resolving With Select Field Name Dot Star",
          "select f1.* from struct.t")*/
        .add(
            "Window Agg With Group By",
            "select min(deptno), rank() over (order by empno), max(empno) over (partition by deptno) from emp group by deptno, empno ")
        .add("Window Average With Group By", "select avg(deptno) over () from emp group by deptno")
        .add(
            "Window Agg With Group By And Join",
            "select min(d.deptno), rank() over (order by e.empno),  max(e.empno) over (partition by e.deptno) from emp e, dept d where e.deptno = d.deptno group by d.deptno, e.empno, e.deptno ")
        .add(
            "Window Agg With Group By And Having",
            "select min(deptno), rank() over (order by empno), max(empno) over (partition by deptno) from emp group by deptno, empno having empno < 10 and min(deptno) < 20 ")
        .add(
            "Window Agg In Sub Query Join",
            "select T.x, T.y, T.z, emp.empno from (select min(deptno) as x,    rank() over (order by empno) as y,    max(empno) over (partition by deptno) as z    from emp group by deptno, empno) as T  inner join emp on T.x = emp.deptno  and T.y = emp.empno ")
        .add(
            "Window over preceding and current row",
            "select T.x, T.y, T.z, emp.empno from (select min(deptno) as x,    sum(empno) over (order by empno rows between 11 preceding and current row) as y,    max(empno) over (partition by deptno) as z    from emp group by deptno, empno) as T  inner join emp on T.x = emp.deptno  and T.y = emp.empno ")
        .add(
            "Window over current row and following",
            "select T.x, T.y, T.z, emp.empno from (select min(deptno) as x,    sum(empno) over (order by empno ROWS BETWEEN CURRENT ROW and 3 following) as y,    max(empno) over (partition by deptno) as z    from emp group by deptno, empno) as T  inner join emp on T.x = emp.deptno  and T.y = emp.empno ")
        .add(
            "Window over preceding and following",
            "select T.x, T.y, T.z, emp.empno from (select min(deptno) as x,    sum(empno) over (order by empno ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as y,    max(empno) over (partition by deptno) as z    from emp group by deptno, empno) as T  inner join emp on T.x = emp.deptno  and T.y = emp.empno ")
        .add(
            "Window over unbounded preceding and current row",
            "select T.x, T.y, T.z, emp.empno from (select min(deptno) as x,    sum(empno) over (order by empno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as y,    max(empno) over (partition by deptno) as z    from emp group by deptno, empno) as T  inner join emp on T.x = emp.deptno  and T.y = emp.empno ")
        .add(
            "Window over unbounded preceding and following",
            "select T.x, T.y, T.z, emp.empno from (select min(deptno) as x,    sum(empno) over (order by empno ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as y,    max(empno) over (partition by deptno) as z    from emp group by deptno, empno) as T  inner join emp on T.x = emp.deptno  and T.y = emp.empno ")
        .add(
            "Order By Over",
            "select deptno, rank() over(partition by empno order by deptno) from emp order by row_number() over(partition by empno order by deptno)")
        .add(
            "Values Using",
            "select d.deptno, min(e.empid) as empid from (values (100, 'Bill', 1)) as e(empid, name, deptno) join (values (1, 'LeaderShip')) as d(deptno, name)   using (deptno) group by d.deptno")
        .add(
            "Correlation Scalar Agg And Filter",
            "SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)")
        .add(
            "Correlation Multi Scalar Aggregate",
            "select sum(e1.empno) from emp e1, dept d1 where e1.deptno = d1.deptno and e1.sal > (select avg(e2.sal) from emp e2   where e2.deptno = d1.deptno)")
        .add(
            "Correlation Exists And Filter",
            "SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and exists (select * from emp e2 where e1.empno = e2.empno)")
        .add(
            "Correlation Not Exists And Filter",
            "SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and not exists (select * from emp e2 where e1.empno = e2.empno)")
        .add(
            "Correlation Aggregate Group Sets",
            "select sum(e1.empno) from emp e1, dept d1 where e1.deptno = d1.deptno and e1.sal > (select avg(e2.sal) from emp e2 where e2.deptno = d1.deptno group by cube(comm, mgr))")
        .add("Fake Star", "SELECT * FROM (VALUES (0, 0)) AS T(A, \"*\")")
        .add("Delete Where", "delete from emp where deptno = 10")
        .add("Delete Bind", "delete from emp where deptno = ?")
        .add("Delete Bind Modifiable View", "delete from EMP_MODIFIABLEVIEW2 where empno = ?")
        .add("Select Without From", "select 2+2")
        /* SOME is only supported if expand = false
        .add(
          "Some",
          "select empno from emp where deptno > some (   select deptno from dept)")
        .add(
          "Some Value List",
          "select empno from emp where deptno > some (10, 20)")
        .add(
          "Some With Equality",
          "select empno from emp where deptno = some (   select deptno from dept)")*/
        .add(
            "Sub Query Or",
            "select * from emp where deptno = 10 or deptno in (     select dept.deptno from dept where deptno < 5) ")
        .add("Update Where", "update emp set empno = empno + 1 where deptno = 10")
        .add("Update Bind", "update emp set sal = sal + ? where slacker = false")
        .add("Update Bind2", "update emp set sal = ? where slacker = false")
        .add(
            "Update Extended Column Collision",
            "update empdefaults(empno INTEGER NOT NULL, deptno INTEGER) set deptno = 1, empno = 20, ename = 'Bob' where deptno = 10")
        .add(
            "Update Extended Column Case Sensitive Collision",
            "update empdefaults(\"slacker\" INTEGER, deptno INTEGER) set deptno = 1, \"slacker\" = 100 where ename = 'Bob'")
        .add(
            "Update Extended Column Modifiable View Collision",
            "update EMP_MODIFIABLEVIEW3(empno INTEGER NOT NULL, deptno INTEGER) set deptno = 20, empno = 20, ename = 'Bob' where empno = 10")
        .add(
            "Update Extended Column Modifiable View Case Sensitive Collision",
            "update EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER, deptno INTEGER) set deptno = 20, \"slacker\" = 100 where ename = 'Bob'")
        .add(
            "Update Extended Column Modifiable View Extended Collision",
            "update EMP_MODIFIABLEVIEW2(\"slacker\" INTEGER, extra BOOLEAN) set deptno = 20, \"slacker\" = 100, extra = true where ename = 'Bob'")
        .add(
            "Update Extended Column Modifiable View Underlying Collision",
            "update EMP_MODIFIABLEVIEW3(extra BOOLEAN, comm INTEGER) set empno = 20, comm = true, extra = true where ename = 'Bob'")
        .add(
            "Update Extended Column",
            "update empdefaults(updated TIMESTAMP) set deptno = 1, updated = timestamp '2017-03-12 13:03:05', empno = 20, ename = 'Bob' where deptno = 10")
        .add(
            "Update Modifiable View",
            "update EMP_MODIFIABLEVIEW2 set sal = sal + 5000 where slacker = false")
        .add(
            "Update Extended Column Modifiable View",
            "update EMP_MODIFIABLEVIEW2(updated TIMESTAMP) set updated = timestamp '2017-03-12 13:03:05', sal = sal + 5000 where slacker = false")
        .add("Delete", "delete from emp")
        .add("Update", "update emp set empno = empno + 1")
        .add(
            "Update Sub Query",
            "update emp set empno = (   select min(empno) from emp as e where e.deptno = emp.deptno)")
        .add("Offset0", "select * from emp offset 0")
        .add(
            "With Inside Scalar Sub Query Rex",
            "select (  with dept2 as (select * from dept where deptno > 10) select count(*) from dept2) as c from emp")
        .add(
            "With Inside Where Exists Rex",
            "select * from emp where exists (   with dept2 as (select * from dept where dept.deptno >= emp.deptno)   select 1 from dept2 where deptno <= emp.deptno)")
        .add(
            "In Uncorrelated Sub Query In Select Rex",
            "select name, deptno in (   select case when true then deptno else null end from emp) from dept")
        .add(
            "Not In Uncorrelated Sub Query In Select Not Null Rex",
            "select empno, deptno not in (   select deptno from dept) from emp")
        .add(
            "Not In Uncorrelated Sub Query Rex",
            "select empno from emp where deptno not in (select deptno from dept)")
        .add(
            "Not Case In Three Clause",
            "select empno from emp where not case when             true then deptno in (10,20) else false end")
        .add(
            "Not Case In More Clause",
            "select empno from emp where not case when              true then deptno in (10,20) when false then false else deptno in (30,40) end")
        .add(
            "Not Case In Without Else",
            "select empno from emp where not case when             true then deptno in (10,20) end")
        .add(
            "Not In Uncorrelated Sub Query In Select Rex",
            "select empno, deptno not in (   select case when true then deptno else null end from dept) from emp")
        .add(
            "In Uncorrelated Sub Query Rex",
            "select empno from emp where deptno in (select deptno from dept)")
        .add(
            "With Inside Where Exists Decorrelate Rex",
            "select * from emp where exists (   with dept2 as (select * from dept where dept.deptno >= emp.deptno)   select 1 from dept2 where deptno <= emp.deptno)")
        .add(
            "Composite In Uncorrelated Sub Query Rex",
            "select empno from emp where (empno, deptno) in (select deptno - 10, deptno from dept)")
        /* NPE
        .add(
          "Join On In Sub Query",
          "select * from emp left join dept on emp.empno = 1 or dept.deptno in (select deptno from emp where empno > 5)")
        .add(
          "Join On Exists",
          "select * from emp left join dept on emp.empno = 1 or exists (select deptno from emp where empno > dept.deptno + 5)")*/
        .add(
            "In Uncorrelated Sub Query In Having Rex",
            "select sum(sal) as s from emp group by deptno having count(*) > 2 and deptno in (   select case when true then deptno else null end from emp)")
        /* assertion error
        .add(
          "Uncorrelated Scalar Sub Query In Group Order Rex",
          "select sum(sal) as s from emp group by deptno order by (select case when true then deptno else null end from emp) desc,   count(*)")
        .add(
          "Uncorrelated Scalar Sub Query In Order Rex",
          "select ename from emp order by (select case when true then deptno else null end from emp) desc,   ename")
        .add(
          "Uncorrelated Scalar Sub Query In Aggregate Rex",
          "select sum((select min(deptno) from emp)) as s from emp group by deptno ")*/
        .add(
            "Union In From",
            "select x0, x1 from (   select 'a' as x0, 'a' as x1, 'a' as x2 from emp   union all   select 'bb' as x0, 'bb' as x1, 'bb' as x2 from dept)")
        /* Ambiguous Column
        .add(
          "Where In Correlated",
          "select empno from emp as e join dept as d using (deptno) where e.sal in (   select e2.sal from emp as e2 where e2.deptno > e.deptno)")*/
        .add(
            "Lateral Decorrelate Rex",
            "select * from emp,  LATERAL (select * from dept where emp.deptno=dept.deptno)")
        .add(
            "Lateral Decorrelate Theta Rex",
            "select * from emp,  LATERAL (select * from dept where emp.deptno < dept.deptno)")
        .add(
            "Exists Correlated Decorrelate Rex",
            "select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno)")
        .add(
            "Correlation Scalar Agg And Filter Rex",
            "SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)")
        .add(
            "Exists Correlated Limit Decorrelate Rex",
            "select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno limit 1)")
        .add(
            "Correlation Exists And Filter Rex",
            "SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and exists (select * from emp e2 where e1.empno = e2.empno)")
        .add(
            "Correlation Exists And Filter Theta Rex",
            "SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and exists (select * from emp e2 where e1.empno < e2.empno)")
        .add(
            "Correlation Join Rex",
            "select *,   multiset(select * from emp where deptno=dept.deptno) as empset from dept")
        .add("Multiset Of Columns Rex", "select 'abc',multiset[deptno,sal] from emp")
        .add(
            "Not Exists Correlated",
            "select * from emp where not exists (   select 1 from dept where emp.deptno=dept.deptno)")
        .add("Select From Dynamic Table", "select n_nationkey, n_name from SALES.NATION")
        .add("Select Star From Dynamic Table", "select * from SALES.NATION")
        .add(
            "Refer Dynamic Star In Select OB",
            "select n_nationkey, n_name from (select * from SALES.NATION) order by n_regionkey")
        .add(
            "Dynamic Star In Table Join",
            "select * from  (select * from SALES.NATION) T1,  (SELECT * from SALES.CUSTOMER) T2  where T1.n_nationkey = T2.c_nationkey")
        .add(
            "Dynamic Nested Column",
            "select * from  (select * from SALES.NATION) T1,  (SELECT * from SALES.CUSTOMER) T2  where T1.n_nationkey = T2.c_nationkey")
        .add(
            "Refer Dynamic Star In Select Where GB",
            "select n_regionkey, count(*) as cnt from (select * from SALES.NATION) where n_nationkey > 5 group by n_regionkey")
        .add(
            "Dynamic Star In Join And Sub Q",
            "select * from  (select * from SALES.NATION T1,  SALES.CUSTOMER T2 where T1.n_nationkey = T2.c_nationkey)")
        .add(
            "Star Join Static Dyn Table",
            "select * from SALES.NATION N, SALES.REGION as R where N.n_regionkey = R.r_regionkey")
        .add(
            "Grp By Col From Star In Sub Query",
            "SELECT n.n_nationkey AS col  from (SELECT * FROM SALES.NATION) as n  group by n.n_nationkey")
        .add(
            "Dyn Star In Exist Sub Q",
            "select * from SALES.REGION where exists (select * from SALES.NATION)")
        .add("Select Dynamic Star Order By", "SELECT * from SALES.NATION order by n_nationkey")
        .add(
            "In To Semi Join",
            "SELECT empno FROM emp AS e WHERE cast(e.empno as bigint) in (130, 131, 132, 133, 134)")
        .add(
            "Window On Dynamic Star",
            "SELECT SUM(n_nationkey) OVER w FROM (SELECT * FROM SALES.NATION) subQry WINDOW w AS (PARTITION BY REGION ORDER BY n_nationkey)")
        .add(
            "With Exists",
            "with t (a, b) as (select * from (values (1, 2))) select * from t where exists (   select 1 from emp where deptno = t.a)")
        .add(
            "Join Subquery With Key Column On The Left",
            " SELECT outerEmp.deptno, outerEmp.sal FROM dept LEFT JOIN emp outerEmp ON outerEmp.deptno = dept.deptno AND dept.deptno IN (   SELECT emp_in.deptno   FROM emp emp_in) ")
        .add(
            "Within Group1",
            "select deptno, collect(empno) within group (order by deptno, hiredate desc) from emp group by deptno")
        .add("Contains", "select * from emp where contains(deptno:1234)")
        /* Ambiguous Column
        .add(
          "Within Group2",
          "select dept.deptno, collect(sal) within group (order by sal desc) as s, collect(sal) within group (order by 1)as s1, collect(sal) within group (order by sal) filter (where sal > 2000) as s2 from emp join dept using (deptno) group by dept.deptn")*/
        .runTests();
  }

  @Test
  public void testTpchQueries() throws URISyntaxException, IOException {
    GoldenFileTestBuilder<MultiLineString, Output, MultiLineString> builder =
        GoldenFileTestBuilder.<MultiLineString, Output>create(TestSerializerRoundtrip::executeTest)
            .allowExceptions();

    for (Path path : getQueryFilePaths()) {
      String description = path.getFileName().toString();
      String query = getQuery(path);

      builder.add(description, MultiLineString.create(query));
    }

    builder.runTests();
  }

  @Test
  public void testSerDeForUnCollect() {
    GoldenFileTestBuilder.<String, Output>create(TestSerializerRoundtrip::executeTest)
        .allowExceptions()
        .add("A simple Uncollect", "select x from unnest(Array[1,2,3]) as t(x)")
        .runTests();
  }

  /**
   * Test deserialization does not take too long time (30+ seconds) after optimizing deserializer to
   * initialize {@code SqlOperatorConverter} only once.
   */
  @Test(timeout = 12000)
  public void testDeserializeTimeWithManyRexCalls() {
    String queryText =
        "with t1 as (\n"
            + "SELECT \n"
            + "case when empno + 1 in (1) then '0' else '9999' end c1,\n"
            + "case when empno + 1 in (2) then '1' else '9999' end c2,\n"
            + "case when empno + 1 in (3) then '2' else '9999' end c3,\n"
            + "case when empno + 1 in (4) then '3' else '9999' end c4,\n"
            + "case when empno + 1 in (5) then '4' else '9999' end c5,\n"
            + "case when empno + 1 in (6) then '5' else '9999' end c6,\n"
            + "case when empno + 1 in (7) then '6' else '9999' end c7,\n"
            + "case when empno + 1 in (8) then '7' else '9999' end c8,\n"
            + "case when empno + 1 in (9) then '8' else '9999' end c9,\n"
            + "case when empno + 1 in (10) then '9' else '9999' end c10\n"
            + "from  EMP),\n"
            + "\n"
            + "t2 as(\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1\n"
            + "union all\n"
            + "SELECT * FROM t1),\n"
            + "\n"
            + "t3 as (\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2\n"
            + "union all\n"
            + "SELECT * FROM t2)\n"
            + "\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n"
            + "union all\n"
            + "SELECT * FROM t3\n";
    executeTest(queryText);
  }

  private static Output executeTest(MultiLineString multiLineQueryText) {
    return executeTest(multiLineQueryText.toString());
  }

  private static Output executeTestWithOpeartorExpansion(String queryText) {
    MockDremioQueryParser tool = new MockDremioQueryParser(OPERATOR_TABLE, CATALOG, "user1");
    RelNode root = tool.toRel(queryText);

    RuleSet rules =
        RuleSets.ofList(
            CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
            REDUCE_FUNCTIONS_FOR_GROUP_SETS,
            GROUP_SET_TO_CROSS_JOIN_RULE_ROLLUP,
            GROUPSET_TO_CROSS_JOIN_RULE,
            REWRITE_PROJECT_TO_FLATTEN_RULE);
    HepPlanner hepPlanner = buildPlannerWithRules(rules);
    hepPlanner.setRoot(root);

    root = hepPlanner.findBestExp();

    String queryPlanText = RelOptUtil.toString(root);
    byte[] queryPlanBinary =
        FACTORY.getSerializer(root.getCluster(), OPERATOR_TABLE).serializeToBytes(root);

    MockDremioQueryParser tool2 = new MockDremioQueryParser(OPERATOR_TABLE, CATALOG, "user1");
    RelNode newRoot =
        FACTORY
            .getDeserializer(tool2.getCluster(), CATALOG_READER, OPERATOR_TABLE, null)
            .deserialize(queryPlanBinary);

    Assert.assertEquals(
        "Query did not roundtrip: " + queryText, queryPlanText, RelOptUtil.toString(newRoot));

    return Output.create(queryPlanText, queryPlanBinary);
  }

  private static HepPlanner buildPlannerWithRules(RuleSet rules) {
    final HepProgramBuilder hepPgmBldr = new HepProgramBuilder();

    int matchLimit = PlannerSettings.HEP_PLANNER_MATCH_LIMIT.getDefault().getNumVal().intValue();
    hepPgmBldr.addMatchLimit(matchLimit);

    MatchCountListener matchCountListener =
        new MatchCountListener(
            0, Iterables.size(rules), matchLimit, Thread.currentThread().getName());

    hepPgmBldr.addMatchOrder(HepMatchOrder.ARBITRARY);
    hepPgmBldr.addRuleCollection(Lists.newArrayList(rules));

    ClusterResourceInformation clusterResourceInformation = mock(ClusterResourceInformation.class);
    when(clusterResourceInformation.getExecutorNodeCount()).thenReturn(1);

    return new DremioHepPlanner(
        hepPgmBldr.build(),
        new PlannerSettings(
            DremioTest.DEFAULT_SABOT_CONFIG,
            ShamOptionResolver.DEFAULT_VALUES,
            () -> clusterResourceInformation),
        new DremioCost.Factory(),
        null,
        matchCountListener);
  }

  private static Output executeTest(String queryText) {
    MockDremioQueryParser tool = new MockDremioQueryParser(OPERATOR_TABLE, CATALOG, "user1");
    RelNode root = tool.toRelWithExpansion(queryText);

    String queryPlanText = RelOptUtil.toString(root);
    byte[] queryPlanBinary =
        FACTORY.getSerializer(root.getCluster(), OPERATOR_TABLE).serializeToBytes(root);

    MockDremioQueryParser tool2 = new MockDremioQueryParser(OPERATOR_TABLE, CATALOG, "user1");
    RelNode newRoot =
        FACTORY
            .getDeserializer(tool2.getCluster(), CATALOG_READER, OPERATOR_TABLE, null)
            .deserialize(queryPlanBinary);

    Assert.assertEquals(
        "Query did not roundtrip: " + queryText, queryPlanText, RelOptUtil.toString(newRoot));

    return Output.create(queryPlanText, queryPlanBinary);
  }

  private static List<Path> getQueryFilePaths() throws URISyntaxException, IOException {
    URI uri = Resources.getResource("queries/tpch").toURI();
    if (!"jar".equals(uri.getScheme())) {
      return getFilesInDirectory(Paths.get(uri));
    }

    synchronized (getLock(uri)) {
      try (FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
        return getFilesInDirectory(fileSystem.getPath("queries/tpch"));
      }
    }
  }

  private static List<Path> getFilesInDirectory(Path path) throws IOException {
    try (Stream<Path> stream = Files.walk(path)) {
      return stream.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
    }
  }

  private static Object getLock(URI uri) {
    String schemeSpecificPart = uri.getSchemeSpecificPart();
    String fileName = schemeSpecificPart.substring(0, schemeSpecificPart.indexOf("!"));
    fileSystemLocks.computeIfAbsent(fileName, s -> new Object());
    return fileSystemLocks.get(fileName);
  }

  private static String getQuery(Path path) throws IOException {
    final URL url = Resources.getResource("queries/tpch/" + path.getFileName().toString());
    return Resources.toString(url, UTF_8).replace(";", " ").replaceAll("\\r\\n?", "\n");
  }

  private static MockDremioTable createTable(
      ImmutableList<ColumnSchema> tableSchema, String... pathTokens) {
    return MockDremioTableFactory.createFromSchema(
        new NamespaceKey(Arrays.stream(pathTokens).collect(Collectors.toList())),
        JavaTypeFactoryImpl.INSTANCE,
        tableSchema);
  }

  /** Output for TestSerializerRoundtrip golden file testcase */
  public static final class Output {
    private final MultiLineString queryPlanText;
    private final Base64String queryPlanBinary;
    private final MultiLineString queryPlanBinaryHexDump;

    @JsonCreator
    private Output(
        @JsonProperty("queryPlanText") MultiLineString queryPlanText,
        @JsonProperty("queryPlanBinary") Base64String queryPlanBinary,
        @JsonProperty("queryPlanBinaryHexDump") MultiLineString queryPlanBinaryHexDump) {
      assert queryPlanText != null;
      assert queryPlanBinary != null;
      assert queryPlanBinaryHexDump != null;

      this.queryPlanText = queryPlanText;
      this.queryPlanBinary = queryPlanBinary;
      this.queryPlanBinaryHexDump = queryPlanBinaryHexDump;
    }

    public MultiLineString getQueryPlanText() {
      return this.queryPlanText;
    }

    public Base64String getQueryPlanBinary() {
      return this.queryPlanBinary;
    }

    public MultiLineString getQueryPlanBinaryHexDump() {
      return this.queryPlanBinaryHexDump;
    }

    public static Output create(String queryPlanText, byte[] queryPlanBinary) {
      String queryPlanBinaryHexDump = HexDump.dump(queryPlanBinary, 0, 0);
      return new Output(
          MultiLineString.create(queryPlanText),
          Base64String.create(queryPlanBinary),
          MultiLineString.create(queryPlanBinaryHexDump));
    }
  }
}
