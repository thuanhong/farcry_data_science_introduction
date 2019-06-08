/* waypoint 43 */
SELECT match.match_id, match.start_time, match.end_time, table3.player_count, table3.kill_suicide_count
FROM match
INNER JOIN (SELECT table1.match_id, table2.player_count, table1.kill_suicide_count
            FROM (SELECT match_id, count(killer_name) as kill_suicide_count
                  FROM match_frag
                  GROUP BY match_id) as table1
            INNER JOIN (SELECT match_id, count(player_name) as player_count
                        FROM (SELECT match_id, killer_name AS player_name
                              FROM match_frag
							UNION
							SELECT match_id, victim_name
							FROM match_frag) as t
                        GROUP BY match_id) as table2
                        ON table1.match_id = table2.match_id) as table3
ON match.match_id = table3.match_id
ORDER BY match.start_time ASC;