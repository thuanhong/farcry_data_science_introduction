/* waypoint 40 */
select killer_name as player_name, count(killer_name) as kill_count 
from match_frag
group by player_name
order by kill_count DESC;