/* waypoint 35 */
select count(frag_time) as kill_count
from match_frag
where victim_name is not null