/* waypoint 43 */
select match.match_id, match.start_time, match.end_time, count(distinct match_frag.killer_name) as player_count, count(match_frag.frag_time) as kill_suicide_count
from match
INNER join match_frag on match.match_id = match_frag.match_id
group by match.match_id;