CREATE TABLE IF NOT EXISTS match
(
    match_id   uuid           NOT NULL DEFAULT uuid_generate_v1(),
    start_time timestamptz(3) NOT NULL,
    end_time   timestamptz(3) NOT NULL,
    game_mode  text           NOT NULL,
    map_name   text           NOT NULL
);

CREATE TABLE IF NOT EXISTS match_frags
(
    match_id    uuid        NOT NULL,
    frag_time   timestamptz(3) NOT NULL,
    killer_name text        NOT NULL,
    victim_name text,
    weapon_code text
);

ALTER TABLE match
    ADD CONSTRAINT pk_match_match_id
        PRIMARY KEY (match_id);

ALTER TABLE match_frags
    ADD CONSTRAINT fk_match_frag_match_id
        FOREIGN KEY (match_id) REFERENCES match (match_id)
            ON UPDATE CASCADE ON DELETE RESTRICT;