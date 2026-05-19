// src/routes/mapConfig.route.ts
import { Elysia } from 'elysia';
import { CreateApiKeyBody, apiKeyParamsSchema, updateApiKeyBodySchema } from './type';
import { createApiKey, deleteApiKey, getApiKeyById, getApiKeys, updateApiKey } from './service';
import { authPlugin } from '../../plugins/plugin';

export const mapConfigRoutes = new Elysia({ prefix: '/management' })
  .use(authPlugin)
  .get('/api-keys', async ({ user }) => {
    return {
      data: await getApiKeys(user.id)
    };
  })
  .get(
    '/api-keys/:apiKeyId',
    async ({ params, user }) => {
      return {
        data: await getApiKeyById(user.id, params.apiKeyId)
      };
    },
    {
      params: apiKeyParamsSchema
    }
  )
  .post(
    '/api-keys', 
    async ({ body, user, set }) => {
      try {
        const userId = user.id;

        if (!userId) {
          set.status = 401;
          return { error: 'Unauthorized' };
        }

        // ส่งข้อมูลให้ Service จัดการ
        const result = await createApiKey({
          userId: userId,
          provider: body.provider,
          keyName: body.keyName,
          keyValue: body.keyValue
        });

        return {
          data: result
        }; // ส่ง apiKey กลับให้ Frontend เอาไปแสดง/copy หลังสร้าง

      } catch (error: any) {
        set.status = 400;
        return { 
          error: "Bad Request",
          message: error.message 
        };
      }
    },
    {
      // ใช้ Schema ตัวเดียวกันเพื่อ Validate และทำ Swagger Docs
      body: CreateApiKeyBody 
    }
  )
  .put(
    '/api-keys/:apiKeyId',
    async ({ params, body, user }) => {
      const updatedApiKey = await updateApiKey({
        userId: user.id,
        apiKeyId: params.apiKeyId,
        keyName: body.keyName,
        isActive: body.isActive
      });

      return {
        data: updatedApiKey
      };
    },
    {
      params: apiKeyParamsSchema,
      body: updateApiKeyBodySchema
    }
  )
  .delete(
    '/api-keys/:apiKeyId',
    async ({ params, user }) => {
      return await deleteApiKey(user.id, params.apiKeyId);
    },
    {
      params: apiKeyParamsSchema
    }
  );
