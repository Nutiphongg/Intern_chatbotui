ALTER TABLE "messages"
ADD COLUMN IF NOT EXISTS "is_silent_retry" BOOLEAN NOT NULL DEFAULT false;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'messages'
      AND column_name = 'is_slilent_retry'
  ) THEN
    EXECUTE 'UPDATE "messages" SET "is_silent_retry" = COALESCE("is_slilent_retry", false)';
  END IF;
END $$;
