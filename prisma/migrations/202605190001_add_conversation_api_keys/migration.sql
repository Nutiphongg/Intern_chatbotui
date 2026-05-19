CREATE TABLE IF NOT EXISTS "conversation_api_keys" (
  "id" TEXT NOT NULL,
  "conversationId" TEXT NOT NULL,
  "userApiKeyId" TEXT NOT NULL,
  CONSTRAINT "conversation_api_keys_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "conversation_api_keys_conversationId_key"
ON "conversation_api_keys"("conversationId");

CREATE INDEX IF NOT EXISTS "idx_conversation_api_keys_user_api_key_id"
ON "conversation_api_keys"("userApiKeyId");

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_conversation_api_keys_conversation'
  ) THEN
    ALTER TABLE "conversation_api_keys"
    ADD CONSTRAINT "fk_conversation_api_keys_conversation"
    FOREIGN KEY ("conversationId")
    REFERENCES "conversations"("id")
    ON DELETE CASCADE
    ON UPDATE NO ACTION;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_conversation_api_keys_user_apikey'
  ) THEN
    ALTER TABLE "conversation_api_keys"
    ADD CONSTRAINT "fk_conversation_api_keys_user_apikey"
    FOREIGN KEY ("userApiKeyId")
    REFERENCES "user_apikey"("id")
    ON DELETE CASCADE
    ON UPDATE NO ACTION;
  END IF;
END $$;
