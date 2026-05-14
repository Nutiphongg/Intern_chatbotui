const getRequiredEnv = (key: string): string => {
    const value = process.env[key];
    if (!value || !value.trim()) {
        throw new Error(`Missing required environment variable: ${key}`);
    }
    return value;
};

const getOptionalEnv = (key: string, fallback: string): string => {
    const value = process.env[key];
    return value && value.trim() ? value : fallback;
};

export const env = {
    NODE_ENV: getOptionalEnv('NODE_ENV', 'development'),
    DATABASE_URL: getRequiredEnv('DATABASE_URL'),
    REDIS_URL: getRequiredEnv('REDIS_URL'),
    ACCESS_SECRET: getRequiredEnv('ACCESS_SECRET'),
    REFRESH_SECRET: getRequiredEnv('REFRESH_SECRET'),
    JWT_SECRET: getRequiredEnv("JWT_SECRET"),
    OLLAMA_URL: getRequiredEnv('OLLAMA_URL'),
    VISION_MODEL: getRequiredEnv('VISION_MODEL'),
    ENCRYPTION_KEY:getRequiredEnv('ENCRYPTION_KEY'),
    SUPABASE_URL: getRequiredEnv('SUPABASE_URL'),
    SUPABASE_SERVICE_ROLE_KEY: getRequiredEnv('SUPABASE_SERVICE_ROLE_KEY'),
    SUPABASE_CHAT_ATTACHMENTS_BUCKET: getRequiredEnv('SUPABASE_CHAT_ATTACHMENTS_BUCKET')
};
