-- SQL script to create indexes for User table performance optimization

-- Index for role filter (most important - filters customers)
CREATE INDEX idx_users_role ON users_user(role);

-- Index for sorting by created_at (most common sort)
CREATE INDEX idx_users_created_at ON users_user(created_at);

-- Index for username search
CREATE INDEX idx_users_username ON users_user(username);

-- Index for email search
CREATE INDEX idx_users_email ON users_user(email);

-- Index for mobile_no search
CREATE INDEX idx_users_mobile_no ON users_user(mobile_no);

-- Index for user_type filter
CREATE INDEX idx_users_user_type ON users_user(user_type);

-- Composite index for role + created_at (optimizes the most common query)
CREATE INDEX idx_users_role_created_at ON users_user(role, created_at);

-- Composite index for role + user_type (optimizes filtered queries)
CREATE INDEX idx_users_role_user_type ON users_user(role, user_type);

-- Run this in your MySQL database:
-- mysql -h 2401:4900:938a:1c6b:e58d:5167:b6a5:f667 -u root -p landnest_db < create_indexes.sql
