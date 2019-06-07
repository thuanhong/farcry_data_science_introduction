/* waypoint 41 */
select match_id, killer_name as player_name, count(killer_name) as kill_count 
from match_frag
group by match_id, player_name
order by match_id, kill_count DESC;