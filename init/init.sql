-- Connexion à la base postgres et création de la base si besoin
-- \c postgres
-- DROP DATABASE IF EXISTS spark_streaming_db;
-- CREATE DATABASE spark_streaming_db;
-- \c spark_streaming_db

-- Table business
DROP TABLE IF EXISTS business_table;
CREATE TABLE business_table (
    business_id     TEXT PRIMARY KEY,
    name            VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    categories      TEXT,
    is_open         INTEGER
);

-- Table users
DROP TABLE IF EXISTS user_table;
CREATE TABLE user_table (
    user_id         TEXT PRIMARY KEY,
    name            VARCHAR(255),
    fans            INTEGER,
    friends         TEXT,
    elite           TEXT,
    yelping_since   TIMESTAMP
);

-- Table review
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

-- Table user
DROP TABLE IF EXISTS top_popular_user_table;
CREATE TABLE top_popular_user_table (
    user_id             VARCHAR(64),
    name                VARCHAR(128),
    fans                INTEGER,
    friends             INTEGER,
    rank                INTEGER
);

-- Table category
DROP TABLE IF EXISTS category_table;
CREATE TABLE category_table (
    business_id         VARCHAR(64),
    category            VARCHAR(255)
);

-- Table top_fun_business_table
DROP TABLE IF EXISTS top_fun_business_table;
CREATE TABLE top_fun_business_table (
    business_id         VARCHAR(64),
    total_useful        INTEGER
);

-- Table top_usefull_user_table
DROP TABLE IF EXISTS top_usefull_user_table;
CREATE TABLE top_usefull_user_table (
    user_id             TEXT PRIMARY KEY,
    name                VARCHAR(255),
    total_useful        INTEGER
);

-- Table top_faithful_user_table
DROP TABLE IF EXISTS top_faithful_user_table;
CREATE TABLE top_faithful_user_table (
    user_id             VARCHAR(64),
    business_id         VARCHAR(64),
    review_count        INTEGER,
    rank                INTEGER
);

-- Table top_rated_by_category_table
DROP TABLE IF EXISTS top_rated_by_category_table;
CREATE TABLE top_rated_by_category_table (
    category            VARCHAR(128),
    business_id         VARCHAR(64),
    name                VARCHAR(256),
    city                VARCHAR(128),
    state               VARCHAR(16),
    review_count        INTEGER,
    average_stars       FLOAT,
    rank                INTEGER
);

-- Table top_popular_business_monthly_table
DROP TABLE IF EXISTS top_popular_business_monthly_table;
CREATE TABLE top_popular_business_monthly_table (
    year_month          VARCHAR(7),  -- format 'YYYY-MM'
    business_id         VARCHAR(64),
    review_count        INTEGER,
    rank                INTEGER
);

-- Table top_popular_user_table (corrigée version complète avec score)
DROP TABLE IF EXISTS top_popular_user_table;
CREATE TABLE top_popular_user_table (
    user_id             VARCHAR(64),
    name                VARCHAR(256),
    fans_count          INTEGER,
    friends_count       INTEGER,
    popularity_score    INTEGER,
    rank                INTEGER
);

-- Table apex_predator_user_table
DROP TABLE IF EXISTS apex_predator_user_table;
CREATE TABLE apex_predator_user_table (
    user_id             VARCHAR(64),
    name                VARCHAR(128),
    elite_years         INTEGER
);

-- Table closed_business_rating_stats_table
DROP TABLE IF EXISTS closed_business_rating_stats_table;
CREATE TABLE closed_business_rating_stats_table (
    average_stars       FLOAT,
    review_count        INTEGER
);

-- Table activity_evolution_table
DROP TABLE IF EXISTS activity_evolution_table;
CREATE TABLE activity_evolution_table (
    year_month      VARCHAR(7), -- format 'YYYY-MM'
    reviews_count   INTEGER,
    users_count     INTEGER,
    business_count  INTEGER
);

DROP TABLE IF EXISTS elite_impact_on_rating_table;
CREATE TABLE elite_impact_on_rating_table (
    elite_status     VARCHAR(16), -- 'elite' ou 'non-elite'
    average_stars    FLOAT,
    review_count     INTEGER
);

DROP TABLE IF EXISTS top_categories_table;
CREATE TABLE top_categories_table (
    category    VARCHAR(16),
    count     INTEGER
);
