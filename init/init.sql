\c postgres
DROP DATABASE spark_streaming_db;
CREATE DATABASE spark_streaming_db;
\c spark_streaming_db

-- Table business
CREATE TABLE business_table (
    business_id     TEXT PRIMARY KEY,
    name            VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    categories      TEXT
);

-- Table review
CREATE TABLE review_table (
    review_id TEXT PRIMARY KEY,
    user_id     VARCHAR(64),
    business_id VARCHAR(64),
    stars       DOUBLE PRECISION,
    useful      INTEGER,
    funny       INTEGER,
    cool        INTEGER,
    text        TEXT,
    date        VARCHAR(50),
    id_date     INTEGER
);

-- Table user
CREATE TABLE user_table (
    user_id             TEXT PRIMARY KEY,
    name                VARCHAR(255)
);

-- Table category
CREATE TABLE category_table (
    business_id VARCHAR(64),
    category    VARCHAR(255)
);

-- Table top_fun_business_table
CREATE TABLE top_fun_business_table (
    business_id     TEXT PRIMARY KEY,
    name            VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    categories      TEXT,
    total_useful    INTEGER
);

-- Table top_fun_business_table
CREATE TABLE top_usefull_user_table (
    user_id             TEXT PRIMARY KEY,
    name                VARCHAR(255),
    total_useful    INTEGER
);
