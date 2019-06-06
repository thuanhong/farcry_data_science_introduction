/* waypoint 49 */
select match_id, killer_name, count(distinct weapon_code) as weapon_count
from match_frag
group by match_id, killer_name;

/* waypoint 50 */
