CREATE TABLE IF NOT EXISTS my_played_tracks(
                song_name VARCHAR(200),
                artist_name VARCHAR(200),
                played_at VARCHAR(200),
                timestamp VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
            )