-- Initialize CMS Pricing database

-- Create database if it doesn't exist
CREATE DATABASE cms_pricing;

-- Create user if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'cms_user') THEN
        CREATE USER cms_user WITH PASSWORD 'cms_password';
    END IF;
END
$$;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE cms_pricing TO cms_user;
GRANT ALL ON SCHEMA public TO cms_user;
