// src/routes/mapConfig.route.ts
import { Elysia } from 'elysia';
import { CreateMapConfigBody,CreateApiKeyBody } from './type';
import { createMapConfig,createApiKey } from './service';
import { authPlugin } from '../../plugins/plugin';

export const mapConfigRoutes = new Elysia({ prefix: '/map' })
  .use(authPlugin)
  .post(
    '/configs', 
    async ({ body, user, set }) => {
      try {
        const userId = user.id;

        if (!userId) {
          set.status = 401;
          return { error: 'Unauthorized' };
        }

        // โยนไปให้ Service ทำงาน
        const result = await createMapConfig({
          userId: userId,
          intentName: body.intentName,
          provider: body.provider,
          baseUrl: body.baseUrl,
          urlTemplate: body.urlTemplate,
          layerConfigTemplate: body.layerConfigTemplate
        });

        // HTTP Status 201 Created ส่ง Data กลับไปเพียวๆ 
        set.status = 201;
        return result; 

      } catch (error: any) {
        // จัดการ Error ที่โยนมาจาก Service (เช่น Provider ไม่ตรง, Config ซ้ำ)
        set.status = 400;
        return { 
          error: "Bad Request",
          message: error.message 
        };
      }
    },
    {
      // ใส่ TypeBox Validation ตรงนี้ 
      // Elysia จะช่วยเช็คให้ก่อนเข้าฟังก์ชัน ถ้าไม่ตรง มันจะส่ง 422 อัตโนมัติ
      body: CreateMapConfigBody 
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

        set.status = 201;
        return result; // ส่ง id และชื่อ key กลับไปให้ Frontend อัปเดตตาราง

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
  );
  /*
  .get('/api-keys', async ({ headers, query, set }) => {
    const userId = headers['x-user-id'] as string;
    if (!userId) {
      set.status = 401;
      return { error: 'Unauthorized' };
    }

    // รองรับการ Filter ด้วย provider เช่น ?provider=VALLARIS
    // มีประโยชน์มากตอนที่ Frontend จะทำ Dropdown ให้เลือก Key เฉพาะเจาะจง
    const filterProvider = query.provider;

    const apiKeys = await prisma.userapikey.findMany({
      where: { 
        userId: userId,
        ...(filterProvider ? { provider: filterProvider } : {}) 
      },
      // [สำคัญมาก Security] เลือกส่งกลับไปเฉพาะข้อมูลที่ปลอดภัย ห้ามส่ง encryptedKey เด็ดขาด!
      select: {
        id: true,
        provider: true,
        keyName: true,
        createdAt: true
      },
      orderBy: { createdAt: 'desc' }
    });

    return apiKeys;
  }, {
    // ให้ Swagger รู้ว่าสามารถแนบ query string ?provider=... มาได้นะ
    query: t.Object({
      provider: t.Optional(t.String()) 
    })
  })

  .delete('/api-keys/:id', async ({ headers, params, set }) => {
    const userId = headers['x-user-id'] as string;
    const keyId = params.id;

    if (!userId) {
      set.status = 401;
      return { error: 'Unauthorized' };
    }

    try {
      // เช็คก่อนว่า Key นี้เป็นของ User คนนี้จริงๆ ป้องกันคนอื่นมั่ว ID มาลบ
      const existingKey = await prisma.userApiKey.findUnique({ where: { id: keyId } });
      
      if (!existingKey || existingKey.userId !== userId) {
        set.status = 404;
        return { error: 'ไม่พบ API Key นี้ หรือคุณไม่มีสิทธิ์ลบ' };
      }

      await prisma.userApiKey.delete({ where: { id: keyId } });

      return { message: 'ลบ API Key สำเร็จ' };
    } catch (error) {
      set.status = 500;
      return { error: 'เกิดข้อผิดพลาดในการลบ' };
    }
  });*/
