// src/interfaces/mapConfig.interface.ts
import { CreateApiKeyDTO, UpdateApiKeyDTO } from '../mapv2/type';

export interface CreateApiKeyPayload extends CreateApiKeyDTO {
  userId: string;
}

export interface UpdateApiKeyPayload extends UpdateApiKeyDTO {
  userId: string;
  apiKeyId: string;
}

export interface ApiKeyIdentityPayload {
  userId: string;
  apiKeyId: string;
}

export interface EncryptedApiKeyRecord {
  id: string;
  provider: string;
  keyName: string;
  encryptedKey: string;
  iv: string;
  isActive: boolean;
  createdAt: Date;
}

export interface ApiKeySummaryResponse {
  id: string;
  provider: string;
  keyName: string;
  maskedKey: string;
  isActive: boolean;
  createdAt: Date;
}

export interface CreatedApiKeyResponse {
  id: string;
  provider: string;
  keyName: string;
  isActive: boolean;
  createdAt: Date;
}

export interface ApiKeyDetailResponse extends ApiKeySummaryResponse {
  apiKey: string;
}

export interface DeleteApiKeyResponse {
  message: string;
}
