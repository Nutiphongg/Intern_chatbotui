// src/interfaces/mapConfig.interface.ts
import { CreateMapConfigDTO,CreateApiKeyDTO } from '../mapv2/type';

// นำข้อมูลที่รับจาก Frontend (DTO) มารวมกับ userId ที่ Backend หามาจาก Token
export interface CreateMapConfigPayload extends CreateMapConfigDTO {
  userId: string;
}

export interface CreateApiKeyPayload extends CreateApiKeyDTO {
  userId: string;
}