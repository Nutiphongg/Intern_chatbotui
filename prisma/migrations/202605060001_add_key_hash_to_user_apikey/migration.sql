ALTER TABLE "user_apikey" ADD COLUMN "keyHash" VARCHAR(64);

CREATE INDEX "idx_user_apikey_key_hash" ON "user_apikey"("keyHash");
