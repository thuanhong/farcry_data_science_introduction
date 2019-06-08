create or replace function calculate_lucky_luke_killers(p_min_count int default 3, p_max_time_between_kills int default 10)
	returns table 
		(
			match_id uuid,
			killer_name text,
			kill_count Int
		)
as $$
declare 
	max_count int;
	current_count int;
	frag_time timestamptz(3);
	row1 REcord;
	row2 record;
begin
	for row1 in (select distinct t1.match_id, t1.killer_name
				 from match_frag as t1
				 order by t1.match_id, t1.killer_name)
		loop
		match_id := row1.match_id;
		killer_name := row1.killer_name;
		max_count := 0;
		current_count := 0;
		for row2 in (select t2.match_id, t2.killer_name, t2.elasped_time
					 from  (select t3.match_id, t3.killer_name, extract(EPOCH from (t3.frag_time-lag(t3.frag_time, 1) over(partition by t3.killer_name order by t3.frag_time))) as elasped_time
							from match_frag as t3 ) as t2
					 where t2.elasped_time is not null and row1.killer_name = t2.killer_name)
			loop
				if (row2.elasped_time <= p_max_time_between_kills)
				then
					current_count = current_count + 1;
				elsif (row2.elasped_time > p_max_time_between_kills)
				then
					if (max_count < current_count)
					then
						max_count := current_count;
					end if;
					current_count := 0;
				end if;
			end loop;
		if (max_count >= p_min_count)
		then
			kill_count := max_count;
			return next;
		end if;
	end loop;
end;
$$ language plpgsql;