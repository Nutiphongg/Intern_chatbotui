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
    GISTDA_API_BASE_URL: getRequiredEnv('GISTDA_API_BASE_URL'),
    VALLARIS_URL: getOptionalEnv(
        'VALLARIS_URL',
        'https://vallaris.dragonfly.gistda.or.th/core/api/tiles/1.0-beta/tiles/66b43475dcc3ef870b9c9f27'
    ),
    VALLARIS_API_KEY: getOptionalEnv('VALLARIS_API_KEY', ''),
    

};
