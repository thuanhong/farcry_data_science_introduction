/* waypoint 42 */
select match_id, victim_name as player_name, count(victim_name) as death_count
from match_frag
where player_name is not null
group by match_id, player_name
order by match_id, death_count DESC;