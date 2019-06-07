/* waypoint 37 */
select match_id, count(frag_time) as kill_suicide_count
from match_frag
group by match_id;