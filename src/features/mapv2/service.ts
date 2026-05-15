//usermapconfig
import {prisma} from "../setup/prisma";
import {
  ApiKeyDetailResponse,
  ApiKeySummaryResponse,
  CreatedApiKeyResponse,
  DeleteApiKeyResponse,
  EncryptedApiKeyRecord,
  CreateApiKeyPayload,
  UpdateApiKeyPayload
} from '../mapv2/interface';
import { decrypt, encrypt, hashApiKey } from "../setup/encryption";
import { ulid } from "ulid";
import { Errors } from "../../lib/errors";

const normalizeProvider = (provider: string): string => {
  return provider.trim().toUpperCase();
};

const sameProvider = (left: string, right: string): boolean => {
  return normalizeProvider(left) === normalizeProvider(right);
};

const maskApiKey = (apiKey: string): string => {
  const cleanApiKey = apiKey.trim();
  if (cleanApiKey.length <= 8) {
    return "*".repeat(cleanApiKey.length);
  }

  return `${cleanApiKey.slice(0, 4)}${"*".repeat(Math.max(cleanApiKey.length - 8, 8))}${cleanApiKey.slice(-4)}`;
};

const toSafeApiKeyResponse = (apiKey: EncryptedApiKeyRecord): ApiKeySummaryResponse => {
  const keyValue = decrypt(apiKey.encryptedKey, apiKey.iv);

  return {
    id: apiKey.id,
    provider: apiKey.provider,
    keyName: apiKey.keyName,
    maskedKey: maskApiKey(keyValue),
    isActive: apiKey.isActive,
    createdAt: apiKey.createdAt
  };
};

// userapikey
export const createApiKey = async (data: CreateApiKeyPayload): Promise<CreatedApiKeyResponse> => {
  const provider = normalizeProvider(data.provider);
  const keyName = data.keyName.trim();

  // 1. เช็คว่า User เคยตั้งชื่อ Key นี้ซ้ำใน Provider เดียวกันหรือไม่
  const existingKeys = await prisma.user_apikey.findMany({
    where: {
      userId: data.userId,
      keyName,
      deletedAt: null
    },
    select: {
      provider: true
    }
  });

  if (existingKeys.some((key) => sameProvider(key.provider, provider))) {
    throw new Error(`you have API Key name "${keyName}"  ${provider} `);
  }

  // 2. [พระเอกออกโรง] นำ keyValue ที่ Frontend ส่งมาไปเข้ารหัส 
  const { iv, encryptedKey } = encrypt(data.keyValue);
  const keyHash = hashApiKey(data.keyValue);
  const  id_apikey = ulid();
  // 3. บันทึกลง Database (เก็บแค่ตัวที่เข้ารหัสแล้วกับ IV)
  const newApiKey = await prisma.user_apikey.create({
    data: {
      id: id_apikey,
      userId: data.userId,
      provider,
      keyName,
      encryptedKey: encryptedKey,
      iv: iv,
      keyHash
    }
  });
  return {
    id: newApiKey.id,
    provider: newApiKey.provider,
    keyName: newApiKey.keyName,
    isActive: newApiKey.isActive,
    createdAt: newApiKey.createdAt,
  }
};

export const getApiKeys = async (userId: string): Promise<ApiKeySummaryResponse[]> => {
  const apiKeys = await prisma.user_apikey.findMany({
    where: {
      userId,
      deletedAt: null
    },
    select: {
      id: true,
      provider: true,
      keyName: true,
      encryptedKey: true,
      iv: true,
      isActive: true,
      createdAt: true
    },
    orderBy: { createdAt: "desc" }
  });

  return apiKeys.map(toSafeApiKeyResponse);
};

export const getApiKeyById = async (
  userId: string,
  apiKeyId: string
): Promise<ApiKeyDetailResponse> => {
  const apiKey = await prisma.user_apikey.findFirst({
    where: {
      id: apiKeyId,
      userId,
      deletedAt: null
    },
    select: {
      id: true,
      provider: true,
      keyName: true,
      encryptedKey: true,
      iv: true,
      isActive: true,
      createdAt: true
    }
  });

  if (!apiKey) {
    throw Errors.badRequest("api key not found");
  }

  const keyValue = decrypt(apiKey.encryptedKey, apiKey.iv);

  return {
    ...toSafeApiKeyResponse(apiKey),
    apiKey: keyValue
  };
};

export const updateApiKey = async (data: UpdateApiKeyPayload): Promise<ApiKeySummaryResponse> => {
  if (
    data.keyName === undefined
    && data.isActive === undefined
  ) {
    throw Errors.badRequest("no api key update data found");
  }

  const existingKey = await prisma.user_apikey.findFirst({
    where: {
      id: data.apiKeyId,
      userId: data.userId,
      deletedAt: null
    }
  });

  if (!existingKey) {
    throw Errors.badRequest("api key not found");
  }

  const nextKeyName = data.keyName?.trim() || existingKey.keyName;

  if (nextKeyName !== existingKey.keyName) {
    const duplicateKeys = await prisma.user_apikey.findMany({
      where: {
        userId: data.userId,
        keyName: nextKeyName,
        deletedAt: null
      },
      select: {
        id: true,
        provider: true
      }
    });

    const duplicateKey = duplicateKeys.find((key) => {
      return key.id !== data.apiKeyId && sameProvider(key.provider, existingKey.provider);
    });

    if (duplicateKey) {
      throw Errors.badRequest(`you have API Key name "${nextKeyName}" ${existingKey.provider}`);
    }
  }

  const updatedApiKey = await prisma.user_apikey.update({
    where: { id: data.apiKeyId },
    data: {
      keyName: nextKeyName,
      ...(data.isActive !== undefined ? { isActive: data.isActive } : {})
    },
    select: {
      id: true,
      provider: true,
      keyName: true,
      encryptedKey: true,
      iv: true,
      isActive: true,
      createdAt: true
    }
  });

  return toSafeApiKeyResponse(updatedApiKey);
};

export const deleteApiKey = async (
  userId: string,
  apiKeyId: string
): Promise<DeleteApiKeyResponse> => {
  const deleted = await prisma.user_apikey.updateMany({
    where: {
      id: apiKeyId,
      userId,
      deletedAt: null
    },
    data: {
      deletedAt: new Date(),
      isActive: false
    }
  });

  if (deleted.count === 0) {
    throw Errors.badRequest("api key not found");
  }

  return {
    message: "delete api key success"
  };
};

