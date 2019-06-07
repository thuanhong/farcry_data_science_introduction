/* waypoint 39 */
select match_id, count(frag_time) as suicide_count
from match_frag
where victim_name is null
group by match_id
order by suicide_count ASC;