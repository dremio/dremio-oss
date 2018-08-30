select
  lag(line_no) over(partition by position_id order by sub, employee_id) as "lag"
from dfs."%s/window/b4.p4"
