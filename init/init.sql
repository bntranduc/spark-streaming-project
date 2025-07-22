DROP TABLE IF EXISTS business_table;
-- CREATE TABLE business_table (
--     business_id     TEXT PRIMARY KEY,
--     name            VARCHAR(255),
--     city            VARCHAR(100),
--     address         VARCHAR(100),
--     useful_count    INTEGER,
--     avg_useful      DOUBLE PRECISION,
--     funny_count     INTEGER,
--     avg_funny       DOUBLE PRECISION,
--     avg_stars   DOUBLE PRECISION,
--     state           VARCHAR(50),
--     categories      TEXT,
--     is_open         INTEGER
-- );

-- DROP TABLE IF EXISTS user_table;
-- CREATE TABLE user_table (
--     user_id         TEXT PRIMARY KEY,
--     name            VARCHAR(255),
--     fans            INTEGER,
--     friends         TEXT,
--     avg_stars       DOUBLE PRECISION,
--     elite           TEXT,
--     yelping_since   TIMESTAMP
-- );

DROP TABLE IF EXISTS review_table;
CREATE TABLE review_table (
    review_id       TEXT PRIMARY KEY,
    user_id         VARCHAR(64),
    business_id     VARCHAR(64),
    stars           DOUBLE PRECISION,
    useful          INTEGER,
    funny           INTEGER,
    cool            INTEGER,
    text            TEXT,
    date            VARCHAR(50),
    id_date         INTEGER
);

