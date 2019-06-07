/* waypoint 52 */
CREATE or replace FUNCTION get_killer_class (weapon_code TEXT) 
RETURNS TEXT AS $$
DECLARE class TEXT;
BEGIN
    CASE WHEN weapon_code IN ('Machete', 'Falcon', 'MP5') THEN class='Hitman';
		 WHEN weapon_code IN ('SniperRifle') THEN class='Sniper';
		 WHEN weapon_code IN ('AG36', 'OICW', 'P90', 'M4', 'Shotgun', 'M249') THEN class='Commando';
		 ELSE class='Psychopath';
    END CASE;
    RETURN class;
END; $$
LANGUAGE PLPGSQL;

select match_id, killer_name as player_name, weapon_code, kill_count, get_killer_class(weapon_code)
from (select match_id, killer_name, weapon_code, min(frag_time) as frag_time, count(weapon_code) as kill_count,
			 row_number() over(partition by match_id, killer_name order by count(weapon_code) DESC, min(frag_time) asc) as rank
	  from match_frag
	  where weapon_code is not null
	  group by match_id, killer_name, weapon_code
	  order by match_id ASC, killer_name ASC) as t
where rank = 1;