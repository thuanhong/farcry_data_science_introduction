# waypoint 27
select match_id, start_time, end_time
from match;

# waypoint 28
select match_id, game_mode, map_name
from match;

# waypoint 29
select *
from match;

#waypoint 30
select distinct killer_name
from match_frag;

# waypoint 31
select distinct killer_name
from match_frag
order by killer_name ASC;

#waypoint 32
select count(*)
from match;

#waypoint 33
select count(frag_time) as kill_suicide_count
from match_frag;

#waypoint 34
select count(frag_time) as suicide_count
from match_frag
where victim_name is null

#waypoint 35
select count(frag_time) as kill_count
from match_frag
where victim_name is not null

#waypoint 36
select victim_name as kill_count
from match_frag

#waypoint 37
select match_id, count(frag_time) as kill_suicide_count
from match_frag
group by match_id;

#waypoint 38
select match_id, count(frag_time) as kill_suicide_count
from match_frag
group by match_id
order by kill_suicide_count DESC;

#waypoint 39
select match_id, count(frag_time) as suicide_count
from match_frag
where victim_name is null
group by match_id
order by suicide_count ASC;

#waypoint 40
select killer_name as player_name, count(killer_name) as kill_count 
from match_frag
group by player_name
order by kill_count DESC;

#waypoint 41
select match_id, killer_name as player_name, count(killer_name) as kill_count 
from match_frag
group by match_id, player_name
order by match_id, kill_count DESC;

#waypoint 42
select match_id, victim_name as player_name, count(victim_name) as death_count
from match_frag
where player_name is not null
group by match_id, player_name
order by match_id, death_count DESC;

#waypoint 43
select match.match_id, match.start_time, match.end_time, count(distinct match_frag.killer_name) as player_count, count(match_frag.frag_time) as kill_suicide_count
from match
INNER join match_frag on match.match_id = match_frag.match_id
group by match.match_id;

#waypoint 44
select match_id, player_name, kill_count, suicide_count, death_count,round(kill_count *100.0 / (kill_count + suicide_count + death_count), 2) as efficiency
from   (select match_id, player_name, sum(kill_count) as kill_count, sum(suicide_count) as suicide_count, sum(death_count) as death_count
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
order by match_id ASC, player_name desc;

waypoint 45
