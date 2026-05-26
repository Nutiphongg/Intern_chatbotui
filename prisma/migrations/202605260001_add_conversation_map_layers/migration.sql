CREATE TABLE IF NOT EXISTS "conversation_map_layers" (
    "id" TEXT NOT NULL,
    "conversation_id" TEXT NOT NULL,
    "layer_key" TEXT NOT NULL,
    "title" TEXT,
    "type" TEXT,
    "order" INTEGER NOT NULL DEFAULT 0,
    "visible" BOOLEAN NOT NULL DEFAULT true,
    "layer_payload" JSONB NOT NULL,
    "map_style" JSONB,
    "active_style" TEXT,
    "deleted_at" TIMESTAMP(6),
    "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "conversation_map_layers_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "conversation_map_layers_conversation_layer_key"
ON "conversation_map_layers"("conversation_id", "layer_key");

CREATE INDEX IF NOT EXISTS "idx_conversation_map_layers_conversation_order"
ON "conversation_map_layers"("conversation_id", "order");

CREATE INDEX IF NOT EXISTS "idx_conversation_map_layers_conversation_deleted"
ON "conversation_map_layers"("conversation_id", "deleted_at");

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_conversation_map_layers_conversation'
    ) THEN
        ALTER TABLE "conversation_map_layers"
        ADD CONSTRAINT "fk_conversation_map_layers_conversation"
        FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id")
        ON DELETE CASCADE ON UPDATE NO ACTION;
    END IF;
END $$;
