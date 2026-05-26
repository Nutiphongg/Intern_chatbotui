CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS "conversation_memory_chunks" (
    "id" text PRIMARY KEY,
    "conversation_id" text NOT NULL,
    "user_id" text NULL,
    "message_id" text NULL,
    "source_hash" varchar(64) NOT NULL UNIQUE,
    "chunk_type" varchar(40) NOT NULL,
    "role" varchar(20) NULL,
    "content" text NOT NULL,
    "event_type" varchar(80) NULL,
    "layer_id" text NULL,
    "style_key" text NULL,
    "geometry_type" varchar(40) NULL,
    "metadata" jsonb NULL,
    "embedding" vector(768) NULL,
    "created_at" timestamp(6) NOT NULL DEFAULT now(),
    "updated_at" timestamp(6) NOT NULL DEFAULT now(),
    CONSTRAINT "fk_conversation_memory_chunks_conversation"
        FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_conversation_memory_chunks_user"
        FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_conversation_memory_chunks_message"
        FOREIGN KEY ("message_id") REFERENCES "messages"("id") ON DELETE CASCADE ON UPDATE NO ACTION
);

CREATE INDEX IF NOT EXISTS "idx_conversation_memory_chunks_conversation_created"
    ON "conversation_memory_chunks" ("conversation_id", "created_at" DESC);
CREATE INDEX IF NOT EXISTS "idx_conversation_memory_chunks_user"
    ON "conversation_memory_chunks" ("user_id");
CREATE INDEX IF NOT EXISTS "idx_conversation_memory_chunks_message"
    ON "conversation_memory_chunks" ("message_id");
CREATE INDEX IF NOT EXISTS "idx_conversation_memory_chunks_event"
    ON "conversation_memory_chunks" ("event_type");
CREATE INDEX IF NOT EXISTS "idx_conversation_memory_chunks_layer"
    ON "conversation_memory_chunks" ("layer_id");
CREATE INDEX IF NOT EXISTS "idx_conversation_memory_chunks_style"
    ON "conversation_memory_chunks" ("style_key");
CREATE INDEX IF NOT EXISTS "idx_conversation_memory_chunks_embedding_hnsw"
    ON "conversation_memory_chunks" USING hnsw ("embedding" vector_cosine_ops);
