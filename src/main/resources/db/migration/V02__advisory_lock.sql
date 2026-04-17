
ALTER TABLE file_metadata
    ADD COLUMN IF NOT EXISTS checked TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();

CREATE OR REPLACE FUNCTION hash_key(hash BYTEA) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE hex_string TEXT;
BEGIN
    hex_string := lpad(encode(hash, 'hex'), 16, '0');
    RETURN ('x'||substring(hex_string, char_length(hex_string)-15, 16))::bit(64)::bigint;
END;
$$;
