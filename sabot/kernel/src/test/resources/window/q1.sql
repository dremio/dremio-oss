select
  count(*) over pos_win `count`,
  sum(salary) over pos_win `sum`
from
  dfs.`%s/window/%s`
window pos_win as %s
