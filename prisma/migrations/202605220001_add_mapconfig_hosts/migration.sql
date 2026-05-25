CREATE TABLE IF NOT EXISTS "mapconfig_hosts" (
    "id" TEXT NOT NULL,
    "provider" TEXT NOT NULL,
    "hostname" TEXT NOT NULL,
    "baseUrl" TEXT NOT NULL,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "mapconfig_hosts_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "mapconfig_hosts_provider_hostname_key" ON "mapconfig_hosts"("provider", "hostname");
CREATE INDEX IF NOT EXISTS "mapconfig_hosts_provider_idx" ON "mapconfig_hosts"("provider");

ALTER TABLE "user_apikey"
ADD COLUMN IF NOT EXISTS "hostId" TEXT;

CREATE INDEX IF NOT EXISTS "idx_user_apikey_host_id" ON "user_apikey"("hostId");

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_user_apikey_mapconfig_host'
    ) THEN
        ALTER TABLE "user_apikey"
        ADD CONSTRAINT "fk_user_apikey_mapconfig_host"
        FOREIGN KEY ("hostId") REFERENCES "mapconfig_hosts"("id")
        ON DELETE SET NULL ON UPDATE NO ACTION;
    END IF;
END $$;
