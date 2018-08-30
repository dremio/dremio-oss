select
  count(1) over(partition by position_id order by sub)
from dfs."%s/window/b1.p1"
