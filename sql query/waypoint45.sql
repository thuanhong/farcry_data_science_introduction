/* waypoint 45 */
drop table match_statistics;
create table match_statistics as
select *
from (select match_id, player_name, kill_count, suicide_count, death_count,round(kill_count *100.0 / (kill_count + suicide_count + death_count), 2) as efficiency
	  from (select match_id, player_name, sum(kill_count) as kill_count, sum(suicide_count) as suicide_count, sum(death_count) as death_count
			from (select match_id, killer_name as player_name, count(killer_name) as kill_count, count(case when victim_name is null then 1 else null end) as suicide_count, killer_name=0 as death_count
				  from match_frag
				  group by match_id, player_name
				  union all
				  select match_id, victim_name as player_name, killer_name=0 as kill_count, victim_name=0 as suicide_count, count(victim_name) as death_count
				  from match_frag
				  where victim_name is not null
				  group by match_id, player_name
				  order by match_id, player_name)
			group by match_id, player_name)
	  order by match_id ASC, player_name desc)