--
-- Copyright (C) 2017-2019 Dremio Corporation
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

select 1 from emp
select   cast(empno as Integer) * (INTERVAL '1-1' YEAR TO MONTH) from emp
select  cast(empno as Integer) * (INTERVAL '1:1' HOUR TO MINUTE) from emp
select distinct sal + 5 from emp
select distinct sum(sal) from emp group by deptno
table emp
select empno from emp where deptno in (10, 20)
select empno from emp where deptno in (10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230)
select empno from emp where deptno in (select deptno from dept)
select empno from emp order by empno, empno desc
select empno from emp order by empno desc nulls last
select empno from emp order by empno DESC, empno ASC
select empno + 1 as x, empno - 2 as y from emp order by y
select empno + 1, deptno, empno from emp order by 2.5 desc
select empno + 1 from emp order by deptno asc, empno + 1 desc
select empno + 1, deptno, empno from emp order by -1 desc
select empno + 1, deptno, empno from emp order by 1 + 2 desc
select empno + 1 as empno, empno - 2 as y from emp order by empno + 3
select empno + 1 as x, empno - 2 as y from emp order by y + 3
select empno + 1 as empno, empno - 2 as y from emp order by empno + 3
select empno, sal from emp union all select deptno, deptno from dept order by sal desc, empno asc
select empno, sal from emp union all select deptno, deptno from dept order by empno * sal + 2
select deptno, count(*) from emp group by deptno order by deptno * sum(sal) desc, min(empno)
select distinct empno, deptno + 1 from emp order by deptno + 1 + empno
select empno, sal from emp union all select deptno, deptno from dept order by 2
select * from emp tablesample substitute('DATASET1') where empno > 5
select last_value(deptno) over (order by empno) from emp
values(cast(interval '1' hour as interval hour to second))
SELECT * FROM emp JOIN dept USING (deptno)
SELECT * FROM emp LEFT JOIN (SELECT *, deptno * 5 as empno FROM dept) USING (deptno,empno)
SELECT * FROM emp JOIN dept on emp.deptno = dept.deptno
select * from emp join dept  on emp.deptno = dept.deptno and emp.empno in (1, 3)
select distinct sal + 5, deptno, sal + 5 from emp where deptno < 10
select count(*), sum(sal) from emp where empno > 10
select a + b from (   select deptno, 1 as uno, name from dept ) as d(a, b, c) where c like 'X%'
select t.r AS myRow from (select row(row(1)) r from dept) t
select * from SALES.NATION t1 join SALES.NATION t2 using (n_nationkey)
select grade from (select empno from emp union select deptno from dept),   salgrade
SELECT * FROM emp NATURAL JOIN dept
SELECT * FROM emp NATURAL JOIN (SELECT deptno AS foo, name FROM dept) AS d
SELECT * FROM emp NATURAL JOIN (SELECT deptno, name AS ename FROM dept) AS d
select empno from emp order by empno offset 10 rows fetch next 5 rows only
select empno from emp order by empno offset ? rows fetch next ? rows only
select empno from emp fetch next 5 rows only
select empno from emp fetch next ? rows only
select empno from emp offset 10 rows fetch next 5 rows only
select empno from emp offset ? rows fetch next ? rows only
select empno from emp offset 10 rows
select empno from emp offset ? rows
select * from SALES.NATION t1 join SALES.NATION t2 using (n_nationkey)
select * from emp where deptno < 10 and deptno > 5 and (deptno = 8 or empno < 100)
select * from emp as e join dept as d using (deptno) join emp as e2 using (empno)
SELECT * FROM emp JOIN dept on emp.deptno + 1 = dept.deptno - 2
select "e" from ( select empno as "e", deptno as d, 1 as "e" from EMP)
with emp2 as (select * from emp) select * from emp2
with emp2 as (select * from emp where deptno > 10) select empno from emp2 where deptno < 30 union all select deptno from emp
select * from emp where exists (   with dept2 as (select * from dept where dept.deptno >= emp.deptno)   select 1 from dept2 where deptno <= emp.deptno)
select (  with dept2 as (select * from dept where deptno > 10) select count(*) from dept2) as c from emp
select sum(deptno) from emp
SELECT * FROM emp JOIN dept on dept.deptno = emp.deptno + 0
SELECT * FROM emp JOIN dept on emp.deptno + 0 = dept.deptno
select * from emp,  LATERAL (select * from dept where emp.deptno=dept.deptno)
select * from (select 2+deptno d2, 3+deptno d3 from emp) e  where exists (select 1 from (select deptno+1 d1 from dept) d  where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)  where d4=d.d1 and d5=d.d1 and d6=e.d3))
select * from (select 2+deptno d2, 3+deptno d3 from emp) e  where exists (select 1 from (select deptno+1 d1 from dept) d  where d1=e.d2 and exists (select 2 from (select deptno+4 d4, deptno+5 d5, deptno+6 d6 from dept)  where d4=d.d1 and d5=d.d1 and d6=e.d3))
select * from emp where exists (   with dept2 as (select * from dept where dept.deptno >= emp.deptno)   select 1 from dept2 where deptno <= emp.deptno)
select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno)
select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno limit 1)
select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno limit 1)
select*from dept as d, unnest(multiset[d.deptno * 2])
SELECT   avg(sum(sal) + 2 * min(empno) + 3 * avg(empno))   over (partition by deptno) from emp group by deptno
with emp2 as (select * from emp) select * from emp2 order by deptno
with emp2 as (select empno, deptno as x from emp) select * from emp2 union all select * from emp2 order by empno + x
select name, deptno in (   select case when true then deptno else null end from emp) from dept
select empno, deptno not in (   select case when true then deptno else null end from dept) from emp
select empno from emp where deptno not in (select deptno from dept)
select empno, deptno not in (   select deptno from dept) from emp
select ename from (select * from emp order by sal) a
select "$f2", max(x), max(x + 1) from (values (1, 2)) as t("$f2", x) group by "$f2"
select empno, deptno not in (   select mgr from emp where mgr > 5) from emp
select empno, deptno not in (   select mgr from emp where mgr is not null) from emp
select empno, deptno not in (   select mgr from emp where mgr in (     select mgr from emp where deptno = 10)) from emp
select empno, deptno not in (   select mgr from emp) from emp
select sum(sal) from emp group by grouping sets (deptno)
select sum(deptno) from emp group by ()
select sum(sal) from emp group by (), ()
select deptno, ename, sum(sal) from emp group by grouping sets ((deptno), (ename, deptno)) order by 2
select deptno, ename, sum(sal) from emp group by grouping sets ( rollup(deptno), (ename, deptno)) order by 2
select deptno, ename, sum(sal) from emp group by grouping sets ( (deptno), CUBE(ename, deptno)) order by 2
select deptno, ename, sum(sal) from emp group by grouping sets ( CUBE(deptno), ROLLUP(ename, deptno)) order by 2
select sum(sal) from emp group by sal,   grouping sets (deptno,     grouping sets ((deptno, ename), ename),       (ename)),   ()
select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by grouping sets (a, b), grouping sets (c, d)
select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by grouping sets (a, (a, b)), grouping sets (c), d
select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by rollup(a, b), rollup(c, d)
select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by cube(a, b)
select 1 from (values (1, 2, 3, 4)) as t(a, b, c, d) group by rollup(b, (a, d))
with t(a, b, c, d) as (values (1, 2, 3, 4)) select 1 from t group by rollup(a, b), rollup(c, d)
select a, b, count(*) as c from (values (cast(null as integer), 2)) as t(a, b) group by rollup(a, b)
select count(*) from emp group by substring(ename FROM 1 FOR 1)
select 1 from (values (0, 1, 2, 3, 4)) as t(a, b, c, x, y) group by grouping sets ((a, b), c), grouping sets ((x, y), ())
select   deptno, grouping(deptno), count(*), grouping(empno) from emp group by empno, deptno order by 2
select   deptno, grouping(deptno), count(*), grouping(empno) from emp group by rollup(empno, deptno)
select *   from emp match_recognize   (     partition by job, sal     order by job asc, sal desc, empno     pattern (strt down+ up+)     define       down as down.mgr < PREV(down.mgr),       up as up.mgr > prev(up.mgr)) as mr
select * from emp match_recognize (   partition by job, sal   order by job asc, sal desc   measures MATCH_NUMBER() as match_num,     CLASSIFIER() as var_match,     STRT.mgr as start_nw,     LAST(DOWN.mgr) as bottom_nw,     LAST(up.mgr) as end_nw   pattern (strt down+ up+)   define     down as down.mgr < PREV(down.mgr),     up as up.mgr > prev(up.mgr)) as mr
select * from emp match_recognize (   partition by job   order by sal   measures MATCH_NUMBER() as match_num,     CLASSIFIER() as var_match,     STRT.mgr as start_nw,     LAST(DOWN.mgr) as bottom_nw,     LAST(up.mgr) as end_nw   pattern (strt down+ up+)   define     down as down.mgr < PREV(down.mgr),     up as up.mgr > prev(up.mgr)) as mr
select * from emp match_recognize (   partition by job   order by sal   measures MATCH_NUMBER() as match_num,     CLASSIFIER() as var_match,     STRT.mgr as start_nw,     LAST(DOWN.mgr) as bottom_nw,     LAST(up.mgr) as end_nw   ALL ROWS PER MATCH   pattern (strt down+ up+)   define     down as down.mgr < PREV(down.mgr),     up as up.mgr > prev(up.mgr)) as mr
select *   from emp match_recognize   (     after match skip to next row     pattern (strt down+ up+)     define       down as down.mgr < PREV(down.mgr),       up as up.mgr > NEXT(up.mgr)   ) mr
SELECT * FROM emp MATCH_RECOGNIZE (   MEASURES     STRT.mgr AS start_mgr,     LAST(DOWN.mgr) AS up_days,     LAST(UP.mgr) AS total_days   PATTERN (STRT DOWN+ UP+)   DEFINE     DOWN AS DOWN.mgr < PREV(DOWN.mgr),     UP AS UP.mgr > PREV(DOWN.mgr) ) AS T
SELECT * FROM emp MATCH_RECOGNIZE (   MEASURES     STRT.mgr AS start_mgr,     LAST(DOWN.mgr) AS bottom_mgr,     LAST(UP.mgr) AS end_mgr   ONE ROW PER MATCH   PATTERN (STRT DOWN+ UP+)   DEFINE     DOWN AS DOWN.mgr < PREV(DOWN.mgr),     UP AS UP.mgr > PREV(LAST(DOWN.mgr, 1), 1) ) AS T
select *   from emp match_recognize   (     after match skip to down     pattern (strt down+ up+)     subset stdn = (strt, down)     define       down as down.mgr < PREV(down.mgr),       up as up.mgr > NEXT(up.mgr)   ) mr
SELECT * FROM emp MATCH_RECOGNIZE (   MEASURES     STRT.mgr AS start_mgr,     LAST(DOWN.mgr) AS up_days,     LAST(UP.mgr) AS total_days   PATTERN (STRT DOWN? UP+)   DEFINE     DOWN AS DOWN.mgr < PREV(DOWN.mgr),     UP AS CASE             WHEN PREV(CLASSIFIER()) = 'STRT'               THEN UP.mgr > 15             ELSE               UP.mgr > 20             END ) AS T
select * from EMP where not (ename not in ('Fred') )
select deptno, name from dept
select deptno + deptno from dept
select deptno from EMP_MODIFIABLEVIEW2 where deptno = ?
select extra from EMP_MODIFIABLEVIEW2
SELECT CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END FROM emp GROUP BY (CASE WHEN emp.empno IN (3) THEN 0 ELSE 1 END)
SELECT  empno, EXPR$2, COUNT(empno) FROM (     SELECT empno, deptno AS EXPR$2     FROM emp) GROUP BY empno, EXPR$2
SELECT SUM(CASE WHEN empno IN (3) THEN 0 ELSE 1 END) FROM emp
SELECT SUM(   CASE WHEN deptno IN (SELECT deptno FROM dept) THEN 1 ELSE 0 END) FROM emp
SELECT SUM(SELECT min(deptno) FROM dept) FROM emp
SELECT * FROM SALES.NATION WHERE n_name NOT IN     (SELECT ''      FROM SALES.NATION)
select  (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END),  min(empno) from EMP group by (CASE WHEN (deptno IN (10, 20)) THEN 0 ELSE deptno END)
select deptno from EMP where deptno > (select min(deptno) * 2 + 10 from EMP)
select deptno from EMP where deptno > (values 10)
select deptno from EMP where deptno > (select deptno from EMP order by deptno limit 1)
select deptno from EMP where deptno in (1, 2) or deptno in (1, 2)
select deptno from emp group by deptno having sum(case when deptno in (1, 2) then 0 else 1 end) + sum(case when deptno in (3, 4) then 0 else 1 end) > 10
select sal from emp group by sal having sal in (   select deptno   from dept   group by deptno   having sum(deptno) > 0)
select deptno from emp group by deptno having max(emp.empno) > (SELECT min(emp.empno) FROM emp)
select deptno,   max(emp.empno) > (SELECT min(emp.empno) FROM emp) as b from emp group by deptno
select * from empdefaults where ename = '上海'
select * from emp_20 where empno > 100
select min(deptno), rank() over (order by empno), max(empno) over (partition by deptno) from emp group by deptno, empno
select avg(deptno) over () from emp group by deptno
select min(d.deptno), rank() over (order by e.empno),  max(e.empno) over (partition by e.deptno) from emp e, dept d where e.deptno = d.deptno group by d.deptno, e.empno, e.deptno
select min(deptno), rank() over (order by empno), max(empno) over (partition by deptno) from emp group by deptno, empno having empno < 10 and min(deptno) < 20
select T.x, T.y, T.z, emp.empno from (select min(deptno) as x,    rank() over (order by empno) as y,    max(empno) over (partition by deptno) as z    from emp group by deptno, empno) as T  inner join emp on T.x = emp.deptno  and T.y = emp.empno
select deptno, rank() over(partition by empno order by deptno) from emp order by row_number() over(partition by empno order by deptno)
select d.deptno, min(e.empid) as empid from (values (100, 'Bill', 1)) as e(empid, name, deptno) join (values (1, 'LeaderShip')) as d(deptno, name)   using (deptno) group by d.deptno
SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)
select sum(e1.empno) from emp e1, dept d1 where e1.deptno = d1.deptno and e1.sal > (select avg(e2.sal) from emp e2   where e2.deptno = d1.deptno)
SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and exists (select * from emp e2 where e1.empno = e2.empno)
SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and not exists (select * from emp e2 where e1.empno = e2.empno)
select sum(e1.empno) from emp e1, dept d1 where e1.deptno = d1.deptno and e1.sal > (select avg(e2.sal) from emp e2 where e2.deptno = d1.deptno group by cube(comm, mgr))
SELECT * FROM (VALUES (0, 0)) AS T(A, "*")
delete from emp where deptno = 10
delete from emp where deptno = ?
delete from EMP_MODIFIABLEVIEW2 where empno = ?
select 2+2
select * from emp where deptno = 10 or deptno in (     select dept.deptno from dept where deptno < 5)
update emp set empno = empno + 1 where deptno = 10
update emp set sal = sal + ? where slacker = false
update emp set sal = ? where slacker = false
update empdefaults(empno INTEGER NOT NULL, deptno INTEGER) set deptno = 1, empno = 20, ename = 'Bob' where deptno = 10
update empdefaults("slacker" INTEGER, deptno INTEGER) set deptno = 1, "slacker" = 100 where ename = 'Bob'
update EMP_MODIFIABLEVIEW3(empno INTEGER NOT NULL, deptno INTEGER) set deptno = 20, empno = 20, ename = 'Bob' where empno = 10
update EMP_MODIFIABLEVIEW2("slacker" INTEGER, deptno INTEGER) set deptno = 20, "slacker" = 100 where ename = 'Bob'
update EMP_MODIFIABLEVIEW2("slacker" INTEGER, extra BOOLEAN) set deptno = 20, "slacker" = 100, extra = true where ename = 'Bob'
update EMP_MODIFIABLEVIEW3(extra BOOLEAN, comm INTEGER) set empno = 20, comm = true, extra = true where ename = 'Bob'
update empdefaults(updated TIMESTAMP) set deptno = 1, updated = timestamp '2017-03-12 13:03:05', empno = 20, ename = 'Bob' where deptno = 10
update EMP_MODIFIABLEVIEW2 set sal = sal + 5000 where slacker = false
update EMP_MODIFIABLEVIEW2(updated TIMESTAMP) set updated = timestamp '2017-03-12 13:03:05', sal = sal + 5000 where slacker = false
delete from emp
update emp set empno = empno + 1
update emp set empno = (   select min(empno) from emp as e where e.deptno = emp.deptno)
select * from emp offset 0
select (  with dept2 as (select * from dept where deptno > 10) select count(*) from dept2) as c from emp
select * from emp where exists (   with dept2 as (select * from dept where dept.deptno >= emp.deptno)   select 1 from dept2 where deptno <= emp.deptno)
select name, deptno in (   select case when true then deptno else null end from emp) from dept
select empno, deptno not in (   select deptno from dept) from emp
select empno from emp where deptno not in (select deptno from dept)
select empno from emp where not case when             true then deptno in (10,20) else false end
select empno from emp where not case when              true then deptno in (10,20) when false then false else deptno in (30,40) end
select empno from emp where not case when             true then deptno in (10,20) end
select empno, deptno not in (   select case when true then deptno else null end from dept) from emp
select empno from emp where deptno in (select deptno from dept)
select * from emp where exists (   with dept2 as (select * from dept where dept.deptno >= emp.deptno)   select 1 from dept2 where deptno <= emp.deptno)
select empno from emp where (empno, deptno) in (select deptno - 10, deptno from dept)
select sum(sal) as s from emp group by deptno having count(*) > 2 and deptno in (   select case when true then deptno else null end from emp)
select x0, x1 from (   select 'a' as x0, 'a' as x1, 'a' as x2 from emp   union all   select 'bb' as x0, 'bb' as x1, 'bb' as x2 from dept)
select * from emp,  LATERAL (select * from dept where emp.deptno=dept.deptno)
select * from emp,  LATERAL (select * from dept where emp.deptno < dept.deptno)
select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno)
SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)
select*from emp where exists (   select 1 from dept where emp.deptno=dept.deptno limit 1)
SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and exists (select * from emp e2 where e1.empno = e2.empno)
SELECT e1.empno FROM emp e1, dept d1 where e1.deptno = d1.deptno and e1.deptno < 10 and d1.deptno < 15 and exists (select * from emp e2 where e1.empno < e2.empno)
select *,   multiset(select * from emp where deptno=dept.deptno) as empset from dept
select 'abc',multiset[deptno,sal] from emp
select * from emp where not exists (   select 1 from dept where emp.deptno=dept.deptno)
select n_nationkey, n_name from SALES.NATION
select * from SALES.NATION
select n_nationkey, n_name from (select * from SALES.NATION) order by n_regionkey
select * from  (select * from SALES.NATION) T1,  (SELECT * from SALES.CUSTOMER) T2  where T1.n_nationkey = T2.c_nationkey
select * from  (select * from SALES.NATION) T1,  (SELECT * from SALES.CUSTOMER) T2  where T1.n_nationkey = T2.c_nationkey
select n_regionkey, count(*) as cnt from (select * from SALES.NATION) where n_nationkey > 5 group by n_regionkey
select * from  (select * from SALES.NATION T1,  SALES.CUSTOMER T2 where T1.n_nationkey = T2.c_nationkey)
select * from SALES.NATION N, SALES.REGION as R where N.n_regionkey = R.r_regionkey
SELECT n.n_nationkey AS col  from (SELECT * FROM SALES.NATION) as n  group by n.n_nationkey
select * from SALES.REGION where exists (select * from SALES.NATION)
SELECT * from SALES.NATION order by n_nationkey
SELECT empno FROM emp AS e WHERE cast(e.empno as bigint) in (130, 131, 132, 133, 134)
SELECT SUM(n_nationkey) OVER w FROM (SELECT * FROM SALES.NATION) subQry WINDOW w AS (PARTITION BY REGION ORDER BY n_nationkey)
with t (a, b) as (select * from (values (1, 2))) select * from t where exists (   select 1 from emp where deptno = t.a) SELECT outerEmp.deptno, outerEmp.sal FROM dept LEFT JOIN emp outerEmp ON outerEmp.deptno = dept.deptno AND dept.deptno IN (   SELECT emp_in.deptno   FROM emp emp_in)
select deptno, collect(empno) within group (order by deptno, hiredate desc) from emp group by deptno
SELECT * FROM EMP AT BRANCH branch_name
SELECT * FROM EMP AT COMMIT commit_hash
SELECT * FROM EMP AT TAG tag_name
DROP ROLE asdf
DROP TABLE asdf
DROP VIEW asdf
DROP VDS asdf
DELETE FROM EMP WHERE EMP.NAME = 'Brandon'
SELECT * FROM EMP; SELECT * FROM EMP
CAST(value AS type)
EXTRACT(timeUnit FROM datetime)
POSITION(string1 IN string2)
POSITION(string1 IN string2 FROM integer1)
CONVERT(myValue, type)
TRANSLATE(char_value USING translation_name)
TRANSLATE(inputString, characters, translations)
OVERLAY(string1 PLACING string2 FROM integer1)
OVERLAY(string1 PLACING string2 FROM integer1 FOR integer2)
FLOOR(datetime TO timeUnit)
CEIL(datetime TO timeUnit)
SUBSTRING('hello' FROM 2 FOR 3)
SUBSTRING('hello' FROM 2)
SUBSTRING(EMP.ENAME FROM EMP.NUMBERCOL)
SUBSTRING(EMP.ENAME FROM EMP.NUMERCOL FOR EMP.NUMBER)
SUBSTRING(EMP.ENAME FROM 2 FOR EMP.NUMBER)
SUBSTRING(EMP.ENAME FROM EMP.NUMERCOL FOR 4)
SUBSTRING('helloworld' FROM EMP.NUMERCOL FOR EMP.NUMBER)
TRIM(string1 FROM string2)
TRIM(BOTH string1 FROM string2)
TRIM(LEADING string1 FROM string2)
TRIM(TRAILING string1 FROM string2)
COUNT(emp.name)
COUNT(DISTINCT name)
COUNT(DISTINCT age)
COUNT(DISTINCT height)
CASE value1 WHEN value11 THEN result1 END
CASE value1 WHEN value11 THEN result1 ELSE resultZ END
CASE value1 WHEN value11 THEN result1 WHEN valueN THEN resultN ELSE resultZ END
CASE value1 WHEN value11, value111 THEN result1 WHEN valueN, valueN1 THEN resultN ELSE resultZ END
CASE WHEN condition1 THEN result1 END
CASE WHEN condition1 THEN result1 ELSE resultZ END
CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 END
--
-- COLLECTION FUNCTION CORPUS
--

-- ELEMENT
ELEMENT(myArray)
ELEMENT([1])
ELEMENT(mymultiset)
ELEMENT(MULTISET(SELECT * FROM T))
ELEMENT(MULTISET[e0])
-- CARDINALITY
CARDINALITY(myArray)
CARDINALITY([1])
CARDINALITY(mymultiset)
CARDINALITY(MULTISET(SELECT * FROM T))
CARDINALITY(MULTISET[e0])
-- IS A SET
mymultiset IS A SET
MULTISET(SELECT * FROM T) IS A SET
MULTISET[e0, e1, eN] IS A SET
-- IS NOT A SET
mymultiset IS NOT A SET
MULTISET(SELECT * FROM T) IS NOT A SET
MULTISET[e0, e1, eN] IS NOT A SET
-- IS EMPTY
mymultiset IS EMPTY
MULTISET(SELECT * FROM T) IS EMPTY
MULTISET[e0, e1, eN] IS EMPTY
-- IS NOT EMPTY
mymultiset IS NOT EMPTY
MULTISET(SELECT * FROM T) IS NOT EMPTY
MULTISET[e0, e1, eN] IS NOT EMPTY
-- IS SUBMULTISET OF
mymultiset IS SUBMULTISET OF mymultiset2
MULTISET(SELECT * FROM T) IS SUBMULTISET OF mymultiset2
MULTISET[e0, e1, eN] IS SUBMULTISET OF mymultiset2
mymultiset IS SUBMULTISET OF MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) IS SUBMULTISET OF MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] IS SUBMULTISET OF MULTISET(SELECT * FROM T)
mymultiset IS SUBMULTISET OF MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) IS SUBMULTISET OF MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] IS SUBMULTISET OF MULTISET[e0, e1, eN]
-- IS NOT SUBMULTISET OF
mymultiset IS NOT SUBMULTISET OF mymultiset2
mymultiset IS NOT SUBMULTISET OF MULTISET(SELECT * FROM T)
mymultiset IS NOT SUBMULTISET OF MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) IS NOT SUBMULTISET OF mymultiset2
MULTISET(SELECT * FROM T) IS NOT SUBMULTISET OF MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) IS NOT SUBMULTISET OF MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] IS NOT SUBMULTISET OF mymultiset2
MULTISET[e0, e1, eN] IS NOT SUBMULTISET OF MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] IS NOT SUBMULTISET OF MULTISET[e0, e1, eN]
-- UNION
mymultiset MULTISET UNION mymultiset2
mymultiset MULTISET UNION MULTISET(SELECT * FROM T)
mymultiset MULTISET UNION MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET UNION mymultiset2
MULTISET(SELECT * FROM T) MULTISET UNION MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET UNION MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET UNION mymultiset2
MULTISET[e0, e1, eN] MULTISET UNION MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET UNION MULTISET[e0, e1, eN]
-- UNION ALL
mymultiset MULTISET UNION ALL mymultiset2
mymultiset MULTISET UNION ALL MULTISET(SELECT * FROM T)
mymultiset MULTISET UNION ALL MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET UNION ALL mymultiset2
MULTISET(SELECT * FROM T) MULTISET UNION ALL MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET UNION ALL MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET UNION ALL mymultiset2
MULTISET[e0, e1, eN] MULTISET UNION ALL MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET UNION ALL MULTISET[e0, e1, eN]
-- UNION DISTINCT
mymultiset MULTISET UNION DISTINCT mymultiset2
mymultiset MULTISET UNION DISTINCT MULTISET(SELECT * FROM T)
mymultiset MULTISET UNION DISTINCT MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET UNION DISTINCT mymultiset2
MULTISET(SELECT * FROM T) MULTISET UNION DISTINCT MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET UNION DISTINCT MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET UNION DISTINCT mymultiset2
MULTISET[e0, e1, eN] MULTISET UNION DISTINCT MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET UNION DISTINCT MULTISET[e0, e1, eN]
-- INTERSECT
mymultiset MULTISET INTERSECT mymultiset2
mymultiset MULTISET INTERSECT MULTISET(SELECT * FROM T)
mymultiset MULTISET INTERSECT MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET INTERSECT mymultiset2
MULTISET(SELECT * FROM T) MULTISET INTERSECT MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET INTERSECT MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET INTERSECT mymultiset2
MULTISET[e0, e1, eN] MULTISET INTERSECT MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET INTERSECT MULTISET[e0, e1, eN]
-- INTERSECT ALL
mymultiset MULTISET INTERSECT ALL mymultiset2
mymultiset MULTISET INTERSECT ALL MULTISET(SELECT * FROM T)
mymultiset MULTISET INTERSECT ALL MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET INTERSECT ALL mymultiset2
MULTISET(SELECT * FROM T) MULTISET INTERSECT ALL MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET INTERSECT ALL MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET INTERSECT ALL mymultiset2
MULTISET[e0, e1, eN] MULTISET INTERSECT ALL MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET INTERSECT ALL MULTISET[e0, e1, eN]
-- INTERSECT DISTINCT
mymultiset MULTISET INTERSECT DISTINCT mymultiset2
mymultiset MULTISET INTERSECT DISTINCT MULTISET(SELECT * FROM T)
mymultiset MULTISET INTERSECT DISTINCT MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET INTERSECT DISTINCT mymultiset2
MULTISET(SELECT * FROM T) MULTISET INTERSECT DISTINCT MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET INTERSECT DISTINCT MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET INTERSECT DISTINCT mymultiset2
MULTISET[e0, e1, eN] MULTISET INTERSECT DISTINCT MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET INTERSECT DISTINCT MULTISET[e0, e1, eN]
-- EXCEPT
mymultiset MULTISET EXCEPT mymultiset2
mymultiset MULTISET EXCEPT MULTISET(SELECT * FROM T)
mymultiset MULTISET EXCEPT MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET EXCEPT mymultiset2
MULTISET(SELECT * FROM T) MULTISET EXCEPT MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET EXCEPT MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET EXCEPT mymultiset2
MULTISET[e0, e1, eN] MULTISET EXCEPT MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET EXCEPT MULTISET[e0, e1, eN]
-- EXCEPT ALL
mymultiset MULTISET EXCEPT ALL mymultiset2
mymultiset MULTISET EXCEPT ALL MULTISET(SELECT * FROM T)
mymultiset MULTISET EXCEPT ALL MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET EXCEPT ALL mymultiset2
MULTISET(SELECT * FROM T) MULTISET EXCEPT ALL MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET EXCEPT ALL MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET EXCEPT ALL mymultiset2
MULTISET[e0, e1, eN] MULTISET EXCEPT ALL MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET EXCEPT ALL MULTISET[e0, e1, eN]
-- EXCEPT DISTINCT
mymultiset MULTISET EXCEPT DISTINCT mymultiset2
mymultiset MULTISET EXCEPT DISTINCT MULTISET(SELECT * FROM T)
mymultiset MULTISET EXCEPT DISTINCT MULTISET[e0, e1, eN]
MULTISET(SELECT * FROM T) MULTISET EXCEPT DISTINCT mymultiset2
MULTISET(SELECT * FROM T) MULTISET EXCEPT DISTINCT MULTISET(SELECT * FROM T)
MULTISET(SELECT * FROM T) MULTISET EXCEPT DISTINCT MULTISET[e0, e1, eN]
MULTISET[e0, e1, eN] MULTISET EXCEPT DISTINCT mymultiset2
MULTISET[e0, e1, eN] MULTISET EXCEPT DISTINCT MULTISET(SELECT * FROM T)
MULTISET[e0, e1, eN] MULTISET EXCEPT DISTINCT MULTISET[e0, e1, eN]

--
-- PERIOD CORPUS
--

-- CONSTRUCTION
(myDateTime, myDateTime)
(myDateTime, myInterval)
PERIOD(myDateTime, myDateTime)
PERIOD(myDateTime, myInterval)
-- CONTAINS
PERIOD(myDateTime, myDateTime) CONTAINS (myDateTime, myDateTime)
PERIOD(myDateTime, myDateTime) CONTAINS PERIOD(myDateTime, myDateTime)
(myDateTime, myDateTime) CONTAINS (myDateTime, myDateTime)
(myDateTime, myDateTime) CONTAINS PERIOD(myDateTime, myDateTime)
-- OVERLAPS
PERIOD(myDateTime, myDateTime) OVERLAPS (myDateTime, myDateTime)
PERIOD(myDateTime, myDateTime) OVERLAPS PERIOD(myDateTime, myDateTime)
(myDateTime, myDateTime) OVERLAPS (myDateTime, myDateTime)
(myDateTime, myDateTime) OVERLAPS PERIOD(myDateTime, myDateTime)
-- EQUALS
PERIOD(myDateTime, myDateTime) EQUALS (myDateTime, myDateTime)
PERIOD(myDateTime, myDateTime) EQUALS PERIOD(myDateTime, myDateTime)
(myDateTime, myDateTime) EQUALS (myDateTime, myDateTime)
(myDateTime, myDateTime) EQUALS PERIOD(myDateTime, myDateTime)
-- PRECEDES
PERIOD(myDateTime, myDateTime) PRECEDES (myDateTime, myDateTime)
PERIOD(myDateTime, myDateTime) PRECEDES PERIOD(myDateTime, myDateTime)
(myDateTime, myDateTime) PRECEDES (myDateTime, myDateTime)
(myDateTime, myDateTime) PRECEDES PERIOD(myDateTime, myDateTime)
-- IMMEDIATELY PRECEDES
PERIOD(myDateTime, myDateTime) IMMEDIATELY PRECEDES (myDateTime, myDateTime)
PERIOD(myDateTime, myDateTime) IMMEDIATELY PRECEDES PERIOD(myDateTime, myDateTime)
(myDateTime, myDateTime) IMMEDIATELY PRECEDES (myDateTime, myDateTime)
(myDateTime, myDateTime) IMMEDIATELY PRECEDES PERIOD(myDateTime, myDateTime)
-- SUCCEEDS
PERIOD(myDateTime, myDateTime) SUCCEEDS (myDateTime, myDateTime)
PERIOD(myDateTime, myDateTime) SUCCEEDS PERIOD(myDateTime, myDateTime)
(myDateTime, myDateTime) SUCCEEDS (myDateTime, myDateTime)
(myDateTime, myDateTime) SUCCEEDS PERIOD(myDateTime, myDateTime)
-- IMMEDIATELY SUCCEEDS
PERIOD(myDateTime, myDateTime) IMMEDIATELY SUCCEEDS (myDateTime, myDateTime)
PERIOD(myDateTime, myDateTime) IMMEDIATELY SUCCEEDS PERIOD(myDateTime, myDateTime)
(myDateTime, myDateTime) IMMEDIATELY SUCCEEDS (myDateTime, myDateTime)
(myDateTime, myDateTime) IMMEDIATELY SUCCEEDS PERIOD(myDateTime, myDateTime)

--
-- VALUE OPERATION CORPUS
--
ROW(1)
ROW(1)[1]
ROW(1)['name']
ARRAY(SELECT * FROM T)
ARRAY(SELECT * FROM T)[1]
ARRAY[1]
ARRAY[1, 2, 3][1]
MAP(SELECT * FROM T)
MAP(SELECT * FROM T)['key']
MAP['key', 1]
MAP['key', 1]['key']
