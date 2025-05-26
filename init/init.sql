CREATE TABLE IF NOT EXISTS review_table (
    review_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    business_id UUID NOT NULL,
    stars INT CHECK (stars >= 1 AND stars <= 5),
    date DATE
);
