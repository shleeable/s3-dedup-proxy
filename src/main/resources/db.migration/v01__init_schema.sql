CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE file_mappings(
        uuid            UUID NOT NULL DEFAULT uuid_generate_v4(),
        user_name       TEXT NOT NULL,
        file_key        TEXT NOT NULL,
        hash            BINARY(64) NOT NULL,

        created         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (uuid)
);

CREATE UNIQUE INDEX file_mappings_forward  on file_mappings(user_name, file_key);
CREATE INDEX file_mappings_reverse  on file_mappings(hash);

CREATE TABLE multipart_uploads(
        uuid            UUID NOT NULL DEFAULT uuid_generate_v4(),
        user_name       TEXT NOT NULL,
        file_key        TEXT NOT NULL,
        tempfile        TEXT NOT NULL,

        created         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (uuid)
);
CREATE UNIQUE INDEX multipart_uploads_forward  on multipart_uploads(user_name, file_key);
CREATE INDEX multipart_uploads_reverse  on multipart_uploads(hash);

CREATE TABLE file_metadata(
        hash BINARY(64) NOT NULL,
        size BIGINT     NOT NULL,

        created         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (hash)
);

CREATE TABLE pending_backup(
        hash BINARY(64) NOT NULL,

        created         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),

        PRIMARY KEY (hash)
);
