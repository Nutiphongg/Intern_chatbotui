ALTER TABLE "user_apikey"
ADD COLUMN IF NOT EXISTS "deletedAt" TIMESTAMP(3);

ALTER TABLE "user_apikey"
DROP CONSTRAINT IF EXISTS "UserApiKey_userId_provider_keyName_key";

DROP INDEX IF EXISTS "UserApiKey_userId_provider_keyName_key";

CREATE INDEX IF NOT EXISTS "idx_user_apikey_deleted_at"
ON "user_apikey"("deletedAt");

CREATE INDEX IF NOT EXISTS "idx_user_apikey_user_id"
ON "user_apikey"("user_id");

CREATE INDEX IF NOT EXISTS "idx_user_apikey_user_provider_key_name"
ON "user_apikey"("user_id", "provider", "keyName");

CREATE UNIQUE INDEX IF NOT EXISTS "idx_user_apikey_active_unique_name"
ON "user_apikey"("user_id", LOWER("provider"), "keyName")
WHERE "deletedAt" IS NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_user_apikey_user'
  ) THEN
    ALTER TABLE "user_apikey"
    ADD CONSTRAINT "fk_user_apikey_user"
    FOREIGN KEY ("user_id")
    REFERENCES "users"("id")
    ON DELETE CASCADE
    ON UPDATE NO ACTION;
  END IF;
END $$;
