/* waypoint 51 */
select match_id, victim_name as player_name, killer_name as worst_enemy_name, kill_count
from (select match_id, killer_name, victim_name, min(frag_time) as frag_time, count(killer_name) as kill_count,
			 row_number() over(partition by match_id, victim_name order by count(killer_name) DESC, min(frag_time) asc) as rank
	  from match_frag
	  where victim_name is not null
	  group by match_id, killer_name, victim_name
	  order by match_id ASC, victim_name ASC) as t
where rank = 1;