/* waypoint 49 */
select match_id, killer_name, count(distinct weapon_code) as weapon_count
from match_frag
group by match_id, killer_name;

/* waypoint 50 */
select match_id, killer_name as player_name, victim_name as favorite_victim_name, kill_count
from (select match_id, killer_name, victim_name, min(frag_time) as frag_time, count(victim_name) as kill_count,
			 row_number() over(partition by match_id, killer_name order by count(victim_name) DESC, min(frag_time) asc) as rank
	  from match_frag
	  where victim_name is not null
	  group by match_id, killer_name, victim_name
	  order by match_id ASC, killer_name ASC) as t
where rank = 1;

/* waypoint 51 */
select match_id, victim_name as player_name, killer_name as worst_enemy_name, kill_count
from (select match_id, killer_name, victim_name, min(frag_time) as frag_time, count(killer_name) as kill_count,
			 row_number() over(partition by match_id, victim_name order by count(killer_name) DESC, min(frag_time) asc) as rank
	  from match_frag
	  where victim_name is not null
	  group by match_id, killer_name, victim_name
	  order by match_id ASC, victim_name ASC) as t
where rank = 1;