ALTER TABLE "conversations"
ADD COLUMN IF NOT EXISTS "memory_summary" JSONB,
ADD COLUMN IF NOT EXISTS "memory_summary_updated_at" TIMESTAMP(6);
