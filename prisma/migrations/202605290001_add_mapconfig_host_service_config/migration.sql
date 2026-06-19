ALTER TABLE "mapconfig_hosts"
ADD COLUMN IF NOT EXISTS "serviceConfig" JSONB;
