# ภาพรวมระบบ Backend (Backend System Overview)

การทำงานของ Backend ระบบ Authorization, Chatbot/LLM, เครื่องมือแผนที่, การจัดสไตล์แผนที่, Filter, PMTiles และฟังก์ชันหลักของระบบทั้งหมด

---

## Feature ระบบ

1. [Runtime ของ project](#1-runtime-ของ-project)
2. [ระบบ Authorization](#2-ระบบ-authorization)
3. [ระบบ Chatbot และ LLM](#3-ระบบ-chatbot-และ-llm)
4. [Memory และ RAG](#4-memory-และ-rag)
5. [ระบบ Map Tools](#5-ระบบ-map-tools)
6. [การแก้ไข MapLibre Style](#6-การแก้ไข-maplibre-style)
7. [Filter Flow](#7-filter-flow)
8. [PMTiles Flow](#8-pmtiles-flow)
9. [State ปัจจุบัน, ประวัติ และ Undo](#9-state-ปัจจุบัน-ประวัติ-และ-undo)


---

## 1. Runtime ของ project

Backend ของระบบนี้สร้างบน **Bun + ElysiaJS**

**ไฟล์หลัก:** `src/index.ts`

**หน้าที่ของไฟล์นี้:**
- โหลดการตั้งค่า JWT expiry ด้วย `loadJwtConfig()`
- กำหนดค่า CORS, Cookies, Swagger และการจัดการ Error ระดับ Global
- ลงทะเบียน Route Groups ทั้งหมด
- เปิดรับการเชื่อมต่อที่ Port `3000`

**Route หลักของระบบ:**

| Prefix | ไฟล์ | หน้าที่ |
|---|---|---|
| `/auth` | `src/features/auth/route.ts` | สมัครสมาชิก, เข้าสู่ระบบ, Refresh Token, ออกจากระบบ, Guest Token |
| `/chat` | `src/features/chatbot/route.ts` | Chat Stream, Conversations, Messages, Map Layer State |
| `/management` | `src/features/map/route.ts` | จัดการ Map Hosts และ User API Key |

---

## 2. ระบบ Authorization

ระบบมี **2 โหมด** สำหรับการใช้งาน:

- **โหมด User:** ผู้ใช้ล็อกอินด้วย Email/Password และรับ Access Token + Refresh Token กลับมา
- **โหมด Guest:** Backend สร้าง Guest Identity ชั่วคราว เก็บใน Redis โดยมีอายุการใช้งาน 1 ชั่วโมง(TTL)

>  **สำคัญ:** Guest ไม่สามารถใช้ Map Tools ได้ ระบบตรวจสอบด้วยเงื่อนไข `role === "guest"` ใน `processChatMessageStream()`

### 2.1 Auth Flow

```
Register / Login
   ตรวจสอบหรือ Hash Password ด้วย bcrypt 
   สร้าง JWT Access Token
   สร้าง JWT Refresh Token
   บันทึก Session ลงฐานข้อมูล
   เก็บ Refresh Token ใน httpOnly Cookie
```

### 2.2 ฟังก์ชัน Auth หลัก

**`src/features/auth/service.ts`**

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `Register(data)` | สร้างผู้ใช้ใหม่ หลังตรวจสอบ Email/Username ซ้ำและ Hash Password แล้ว |
| `Login(body, userAgent, ip)` | ตรวจสอบ Credentials, จำกัด Active Sessions, สร้าง Token และบันทึก Session |
| `Refresh(refreshToken, userAgent, ip)` | ตรวจสอบ Refresh Token, หมุนเวียน Token คู่ใหม่, แทนที่ Session เก่า |
| `getActiveDevices(userId)` | นับจำนวน Active Sessions แยกตาม User Agent |
| `Logout(refreshToken)` | ลบ Session ตาม Refresh Token |
| `createGuestUser()` | สร้าง Guest Record ใน Redis พร้อม TTL |
| `verifyGuestUser(guestId)` | อ่านข้อมูล Guest User จาก Redis |

**`src/features/auth/jwt.ts`**

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `loadJwtConfig()` | โหลดค่า Expiry ของ Access/Guest/Refresh Token จาก `systemSettings` |
| `signAccessToken(payload)` | ลงนาม Access Token ของผู้ใช้หรือ Guest |
| `signRefreshToken(payload)` | ลงนาม Refresh Token |
| `verifyAccessToken(token)` | ตรวจสอบ Bearer Token |
| `verifyRefreshToken(token)` | ตรวจสอบ Refresh Token |
| `getUserIdFromToken(token)` | ดึง User ID จาก Refresh Token |

**`src/plugins/plugin.ts`**

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `authPlugin` | Elysia Derive Plugin ที่อ่าน `Authorization: Bearer ...`, ตรวจสอบ Access Token และ expose `user.id` กับ `user.role` ให้ Route ใช้งาน |

### 2.3 ความปลอดภัยของ API Key

API Key ของ Map Provider **ไม่ถูกเก็บเป็น text** ทุกรายการถูกเข้ารหัสก่อนบันทึกลง database

**`src/features/setup/encryption.ts`**

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `encrypt(text)` | เข้ารหัส API Key ด้วย `aes-256-gcm` |
| `decrypt(encryptedText, ivHex)` | ถอดรหัส API Key เมื่อ Backend ต้องการเรียกใช้ Provider API |
| `hashApiKey(text)` | สร้าง Hash แบบ HMAC-SHA256 เพื่อเปรียบเทียบ `x-api-key` โดยไม่ต้องเปิดเผย text |

**`src/features/map/service.ts`**

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `createApiKey(data)` | เข้ารหัสและบันทึก Map API Key ของผู้ใช้|
| `getApiKeys(userId)` | แสดงรายการ API Key แบบ Masked |
| `getApiKeyById(userId, apiKeyId)` | ดึงรายละเอียด API Key รวมถึง Key ที่ถอดรหัสแล้วสำหรับเจ้าของเท่านั้น |
| `updateApiKey(data)` | อัปเดตชื่อ, Host หรือสถานะ Active ของ Key |
| `deleteApiKey(userId, apiKeyId)` | Soft Delete API Key ของผู้ใช้ |
| `getActivehosts()` | แสดงรายการ Map Host ที่ใช้งานอยู่และชื่อ Service Key ที่มีให้ |

---

## 3. ระบบ Chatbot และ LLM

ระบบ Chat ส่งข้อมูลกลับไปยัง Frontend ผ่าน **Server-Sent Events (SSE)**

- **Route :** `POST /chat`
- **Function:** `processChatMessageStream(userId, role, body, apiKey, vectorApiKey)`
- **ไฟล์:** `src/features/chatbot/service.ts`

### 3.1 Chat Stream Flow

```
POST /chat
   authPlugin ระบุตัวตนผู้ใช้
   Route ส่ง x-api-key และ Request Body
   processChatMessageStream()
   สร้าง SSE Stream
   บันทึกข้อความของ user
   วิเคราะห์รูปภาพ ถ้ามีการเพิ่มเข้ามา
   โหลด Conversation Memory และ Map State
   วิเคราะห์ว่า Request นี้เป็น: Normal Chat / Map Option / Map Layer / Style Edit / Filter / Clear / PMTiles / Undo
   เรียก Map Tools ตามการใช้งาน
   Stream Events respone ให้ Frontend
   บันทึก Assistant Response และ Metadata
   อัปเดต Conversation Memory Chunks
```

### 3.2 SSE Events หลัก

| Event | ความหมาย |
|---|---|
| `meta` | Conversation ID, Message ID, โมเดลที่เลือกใช้ |
| `ping` | หลักการ Heartbeat เพื่อรักษา Connection ในคำขอที่ใช้เวลานาน |
| `token` | Text Tokens ที่ Chatbot ส่งกลับมาแบบ Streaming |
| `vision` | ผลลัพธ์จากการวิเคราะห์รูปภาพ |
| `map_access` | Provider/Config ที่ผู้ใช้ปัจจุบันได้รับอนุญาต |
| `map_options` | ตัวเลือกที่ผู้ใช้ต้องเลือกก่อนแสดง Layer |
| `map` | ข้อมูล Layer Catalog |
| `map_style` | MapLibre Style สมบูรณ์สำหรับ Layer ปัจจุบัน |
| `map_style_patch` | Patch ขนาดเล็กสำหรับอัปเดตค่าสไตล์ Attribute |
| `map_filter_patch` | Patch ขนาดเล็กสำหรับเพิ่ม Filter ใน Map Style ปัจจุบัน |
| `attribute_values` | ค่า/สถิติของ Attribute ที่เลือก |
| `suggestions` | Prompt/Action ที่แนะนำสำหรับ UI Frontend |
| `map_error` | ข้อผิดพลาดจาก Map Tool |
| `done` | Stream เสร็จสมบูรณ์ |

### 3.3 Conversation State

มีเก็บข้อมูล 2 ที่ใน db :

| Storage | หน้าที่ |
|---|---|
| `messages.metadata` | ประวัติ Events ทั้งหมด ใช้สำหรับ Audit, ดูประวัติ, Undo และ Memory |
| `conversation_map_layers` | Map State ปัจจุบัน ถือเป็น **Source of Truth** สำหรับการ Render/Edit |

> **สำคัญ:**
> - **State ปัจจุบัน** → ใช้จาก `conversation_map_layers`
> - **ประวัติและ Undo** → ใช้จาก `messages.metadata`

### 3.4 กลุ่มฟังก์ชันใน Chatbot Service

**`src/features/chatbot/service.ts`**

#### Message และ Conversation APIs

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `getAvailableModels()` | ดึงรายการโมเดล Ollama ที่พร้อมใช้งาน |
| `getUserConversations(userId, role, page, limit)` | แสดงรายการ Conversation |
| `getChatHistory(userId, role, conversationId, page, limit)` | แสดงรายการข้อความในการสนทนาพร้อม Hydrate Metadata |
| `deleteConversation(userId, conversationId)` | ลบ Conversation (Soft/Hard Delete ขึ้นกับ Implementation) |
| `editMessage(userId, messageId, newContent, is_generate)` | แก้ไขข้อความ และ Regenerate ได้ถ้าต้องการ |
| `editConvTitle(userId, conversationId, title)` | อัปเดตชื่อ Conversation |
| `migrateGuestChatToUser(userId, guestId)` | ย้าย Conversation ของ Guest ไปยังผู้ใช้ที่ลงทะเบียนแล้ว |

#### Map Layer State

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `ensureConversationMapLayersTable()` | ตรวจสอบว่า Map Layer Table/Columns พร้อมใช้งาน |
| `syncConversationMapLayerCatalog()` | Upsert `layer_catalog` เข้า `conversation_map_layers.layer_payload` |
| `syncConversationMapStyle()` | อัปเดต `conversation_map_layers.map_style` |
| `syncConversationMapClear()` | ทำเครื่องหมาย Layer ที่เลือก/ทั้งหมดว่าถูก Clear แล้ว |
| `getConversationMapLayers(userId, role, conversationId)` | ดึง Map Layer State สำหรับ LayerId ของ conversationId นั้น |
| `updateConversationMapLayerOrder(userId, role, conversationId, body)` | อัปเดตลำดับการเรียง LayerId Style Metadata  |
| `applyMapPayloadToState()` | นำ layerId Catalog ไปใช้กับ ram Request State |
| `applyMapStyleToState()` | นำผลลัพธ์ style ไปใช้กับ ram Request State |

#### Layer Selection สำหรับการแก้ไข

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `selectMapLayerStateForEdit()` | เลือก layerId สำหรับการแก้ไข Style/Filter |
| `selectMapLayerStateByText()` | จับคู่ Layer ด้วย Title/Source/Name |
| `selectMapLayerStateByAttribute()` | เลือก layerId ที่มี Attribute ที่ตรง |
| `scoreLayerTextMatch()` | ให้คะแนนความตรงกันระหว่าง Prompt กับ Layer Metadata |
| `getRequestedLayerIdFromToolArgs()` | อ่าน layerId จาก Tool Args อย่างชัดเจน |
| `getRequestedAttributeNamesForLayerSelection()` | อ่านชื่อ Attribute จาก Prompt/Tool Args |
| `selectMapStyleForEdit()` | หา Style ปัจจุบันที่จะแก้ไข |
| `selectMapPayloadForEdit()` | หา Layer Payload ปัจจุบันที่คู่กับการแก้ไข Style |

**ลำดับการเลือก Layer (Priority):**

1. `layerId` ที่ระบุชัดเจน
2. Layer Title/Source ที่ระบุชัดเจน
3. Layer ที่มี Attribute ที่ต้องการ
4. Layer ที่กล่าวถึงใน Text ของข้อความ
5. Layer ที่ Active ล่าสุด

#### Suggestions 

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `buildMapSuggestionsPayload()` | สร้าง Suggestions สำหรับ Frontend จาก Layer/Style ปัจจุบัน |
| `enrichMapSuggestionsWithAttributeValues()` | เพิ่มคำแนะนำจาก Attribute Values เมื่อมีข้อมูล |
| `splitMapSuggestionsPayload()` | แยก Suggestions จาก Attribute Values เมื่อจำเป็น |
| `collectMapStyleAttributeKeys()` | อ่าน Attribute Keys ที่ใช้ใน MapLibre Style ปัจจุบัน |
| `collectMapStyleAttributeValues()` | อ่านค่าจาก Style Expressions และ Filter Expressions |
| `getMapStyleAttributeVariants()` | อ่าน Attribute Style Variants ที่บันทึกไว้ |
| `mergeMapStyleAttributeVariants()` | รักษา Attribute Style Variants เมื่อมีการแก้ไข Style |

#### Filter Patch Flow

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `buildFilterEditArgsFromInstruction()` | แปลง Prompt/Tool Args เป็น Filter Edit Args ที่มีโครงสร้าง |
| `buildFilterEditArgs()` | สร้าง Filter Args พร้อม Inference ประเภทและค่าของ Attribute |
| `buildMapFilterPatch()` | สร้าง `map_filter_patch` สำหรับส่งข้อมูล filter ให้ Frontend |
| `resolveFilterValuesFromInstruction()` | จับคู่ข้อความใน Prompt กับ Attribute Values ที่ match กัน |
| `findFilterValueTextMatch()` | เลือกค่าที่ตรงกันที่สุดจาก Style/Data Values |

> Filter Patches ควรถูก Stream เป็น `map_filter_patch`
> ส่วน Full Style ยังคงถูกเก็บใน `conversation_map_layers.map_style` แต่ Frontend จะรับ Patch Event แทนสำหรับการอัปเดตที่เบากว่า

#### Map Style History / Undo

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `getMapStyleHistorySelection()` | อ่าน Undo Request จาก `mapselection` |
| `restoreConversationMapLayerStyleFromHistory()` | หา Style เก่าใน Message Metadata และเขียนกลับเข้า Layer State ปัจจุบัน |

---

## 4. Memory และ RAG

Memory/RAG ใช้เป็น **Context สำหรับ Assistant เท่านั้น** ไม่ได้เป็น Source of Truth สำหรับการ Render

**ไฟล์:** `src/features/chatbot/memoryChunks.ts`

### Flow

```
Messages + Metadata
   สร้าง Memory Chunks
   Embed ด้วย Ollama Embedding Model 
   บันทึกใน conversation_memory_chunks
   ค้นหาด้วย pgvector หรือ Keyword
   ใส่เป็น Context ให้ LLM
```

### ฟังก์ชันหลัก

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `saveConversationMemoryChunks({ userId, message })` | สร้าง Chunks จากข้อความ/Events และบันทึก |
| `retrieveConversationMemoryChunks(userId, conversationId, query, limit)` | ดึง Chunks ที่เกี่ยวข้องด้วย Vector Similarity หรือ Keyword Fallback |
| `buildMemoryChunks(message)` | แปลง Message Metadata เป็น Searchable Memory Chunks |
| `fetchEmbedding(content)` | เรียก Ollama Embedding Endpoint |
| `retrieveByKeyword(...)` | ค้นหาแบบ Keyword เมื่อ Vector Search ไม่พร้อมใช้งาน |

### Environment Variables

| ตัวแปร | หน้าที่ |
|---|---|
| `MEMORY_EMBEDDING_MODEL` | ชื่อโมเดล Embedding |
| `MEMORY_RAG_TOP_K` | จำนวน Memory Chunks ที่จะดึงมา |
| `MEMORY_RAG_MATCH_THRESHOLD` | ค่า Minimum Vector Similarity |
| `MEMORY_SUMMARY_MODEL` | โมเดลสำหรับสรุปแบบ Rolling Summary |

> **กฎสำคัญ:** ถ้า Memory ขัดแย้งกับ `conversation_map_layers` จะอิงที่**`conversation_map_layers`** 

---

## 5. ระบบ Map Tools

Map Tools ทั้งหมดอยู่ใน: `src/features/map/tools.ts`

Map Tools ถูกเรียกโดย Chatbot Service โดยอ่านข้อมูลจาก DB-backed `mapconfig`, User API Keys และ Layer/Style State ปัจจุบัน

### 5.1 ประเภทของ Map Tools

| Tool / ฟังก์ชัน | หน้าที่ |
|---|---|
| `handleCheckMapAccess()` | ตรวจสอบ Provider/Host/Config ที่ผู้ใช้ปัจจุบันได้รับอนุญาต |
| `handleMapOptionsTool()` | สร้างรายการ Layer/Options จาก DB Config และ Provider APIs |
| `handleMapTool()` | สร้าง `layer_catalog` payload ขั้นสุดท้าย |
| `handleEditMapStyleTool()` | แก้ไข MapLibre Style ปัจจุบัน |
| `handleMapAttributeValuesTool()` | ดึงค่า value Attribute/สถิติจาก Analytics/Explore API |
| `handleClearMapLayersTool()` | Clear Layer ที่เลือกหรือทั้งหมด |
| `handleRenderPmtilesLayerTool()` | แปลง Vector Layer Payload ปัจจุบันเป็น PMTiles Render Payload |
| `handleStyleCatalogTool()` | โหลด Style Presets และ Color Catalog ที่มีอยู่ |

### 5.2 Provider Config Flow

การตั้งค่า Provider ทั้งหมดจาก db

**ตารางฐานข้อมูล:**

| ตาราง | หน้าที่ |
|---|---|
| `mapconfig` | Intent/Provider URL Templates, Layer Config Template, Style/Value Endpoints |
| `mapconfig_hosts` | Base URL ของ Host และการตั้งค่า Service ระดับ Host |
| `user_apikey` | API Keys ของผู้ใช้ที่เข้ารหัสแยกตาม Provider/Host |
| `conversation_api_keys` | เชื่อม Conversation กับ API Key ที่ใช้ระหว่าง Chat |

**ตัวอย่างค่า Config ใน `layerConfigTemplate`:**

```json
{
  "type": "vector_tile",
  "optionKey": "layerId",
  "pagination": {
    "limit": 5,
    "enabled": true
  },
  "collectionQuery": {
    "itemType": "Tile"
  },
  "detailUrlTemplate": "/core/api/tiles/1.0-beta/tiles/{id}",
  "pmtilesUrlTemplate": "/core/api/tiles/1.0-beta/pmtiles/{id}",
  "attributeValues": {
    "connectionUrlTemplate": "/core/api/analytics/1.0/connections",
    "connectionQuery": { "type": "vallaris11" },
    "exploreUrlTemplate": "/core/api/analytics/1.0/explore",
    "method": "POST",
    "datasourceTemplate": "features_{datasetId}",
    "aggregate": "countd",
    "offset": 0,
    "limit": 10000,
    "suggestionLimit": 24
  }
}
```

> **กฎ:** Backend อ่านค่าจาก DB เฉพาะ Provider

### 5.3 Map Options Flow

**ฟังก์ชัน:** `handleMapOptionsTool()`

```
ผู้ใช้ขอรายการ/ตัวเลือก
  → Resolve User API Keys
  → อ่าน Active Mapconfig
  → Infer Provider/Intent จาก Prompt และ DB Configs ที่มีอยู่
  → ถ้าไม่มี Intent/Provider → Stream map_options พร้อมตัวเลือกให้เลือก
  → สำหรับ Vallaris Collection Config → เรียก Collection Endpoint
  → สร้าง Layer Choices แบบ Paginated
  → Stream map_options
```

**Helper Functions:**

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `resolveUserMapApiKeys()` | หา API Keys ของผู้ใช้จาก DB/Header |
| `filterConfigsByProviders()` | กรอง Mapconfig ตาม Provider ที่อนุญาต |
| `filterConfigsByQuery()` | จับคู่ Prompt Query กับ DB Config |
| `buildVectorTileOptionsPayload()` | สร้างตัวเลือก Vector Tile |
| `getVectorTileCollectionRequestParams()` | สร้าง Collection Request Query จาก Config และ Prompt/Mapselection |
| `buildMapOptionUrl()` | สร้าง Provider URL พร้อม Query และ API Key |

### 5.4 Layer Catalog Flow

**ฟังก์ชัน:** `handleMapTool()`

```
Selected Option / Mapselection
   อ่าน Mapconfig
   Resolve และถอดรหัส User API Key
   Render URL/Template Variables
   ดึงข้อมูลจาก Detail Endpoint เมื่อจำเป็น
   Normalize ข้อมูล Layer (fields, bounds, sourceLayer, geometryType, attributes)
   Stream event: map / layer_catalog
   Sync เข้า conversation_map_layers.layer_payload
```

**Helper Functions:**

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `buildVectorTileLayerPayload()` | สร้าง Vector Tile Layer Payload จาก Layer ID ที่เลือก |
| `buildVallarisLayerPayload()` | สร้าง Non-Collection Vallaris Map Payload |
| `normalizeAttributeFields()` | Normalize Tile Fields เป็น `{ field: { type } }` |
| `findAttributeFieldsInPayload()` | ค้นหา Fields ใน Provider Payload ที่ซ้อนกัน |
| `inferVectorTileGeometryType()` | Sample Vector Tiles เพื่อ Infer Geometry เมื่อ Detail Payload ไม่มีข้อมูล |
| `createVectorTilePublicUrl()` | ลบ Private API Key ออกจาก URL ก่อน Stream |

### 5.5 Attribute Values Flow

**ฟังก์ชัน:** `handleMapAttributeValuesTool()`

```
Style/Filter Request ต้องการ Attribute Values
  → อ่าน attributeValues Config จาก layerConfigTemplate
  → Resolve User/Provider API Key
  → สร้าง Connection URL
  → ดึง Analytics Connection ID
  → สร้าง Explore POST Body
  → เรียก Explore Endpoint
  → สรุปค่าหรือสถิติตัวเลข
  → ส่งค่ากลับสำหรับ Style/Filter/Suggestions
```

**Helper Functions:**

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `getAttributeValuesConfig()` | อ่าน `attributeValues` Config |
| `buildAttributeExploreBody()` | สร้าง Analytics Explore Request Body |
| `extractAttributeValueItems()` | ดึง Rows จาก Explore Response |
| `summarizeAttributeValues()` | แปลง Rows เป็น String Values หรือ Numeric Stats |
| `getConnectionIdFromPayload()` | ดึง connectionId จาก Connections Response |

---

## 6. การแก้ไข MapLibre Style

**ฟังก์ชันหลัก:** `handleEditMapStyleTool()`

ฟังก์ชันนี้รับ `map_style` ปัจจุบันและ Tool Args แล้วคืนค่า Style ใหม่กลับมา

### 6.1 Style Edit Flow

```
LLM เรียก edit_map_style
   Chatbot เลือก Target Layer/Style จาก conversation_map_layers
   ดึง Attribute Values เพิ่มเติมถ้าจำเป็น
   Normalize Edit Args
   handleEditMapStyleTool() ทำ Patch บน Style ปัจจุบัน
   Chatbot Stream map_style หรือ map_style_patch/map_filter_patch
   Backend Sync Style ล่าสุดไปยัง conversation_map_layers
```

### 6.2 Style Helpers หลัก

| ฟังก์ชัน | หน้าที่ |
|---|---|
| `resolveEditMapStyleOperation()` | ตัดสินใจว่า Request เป็น: Update, Add Property, Remove Property หรือ Filter |
| `parseStylePropertyInstruction()` | Parse Style Property ที่ได้จาก Prompt |
| `canPatchStyleProperty()` | ตรวจสอบ Property ตาม MapLibre Style Spec/Layer ปัจจุบัน |
| `applyStylePropertyOperation()` | เพิ่ม/ลบ Paint/Layout Property |
| `getPaintPatchForLayerType()` | สร้าง Paint Patch จาก Explicit Args, Color, Attribute หรือ Generic Property |
| `buildAttributePaintPatch()` | สร้าง Expression Style จาก Attribute Values/Stats |
| `buildHeatmapAttributeStyleLayers()` | สร้าง Heatmap Layers หลายชั้นเมื่อต้องรักษา Attribute Color Categories |
| `applyFilterOperation()` | เพิ่ม Filter Expression ใน Style Layer |
| `isValidFilterExpression()` | ตรวจสอบ Filter Attributes กับ Fields ที่มีอยู่ |

### 6.3 ประเภท Style Output

| Output | ความหมาย |
|---|---|
| `map_style` | Style Payload เต็มรูปแบบสำหรับ Render Layer ปัจจุบัน |
| `map_style_patch` | Patch ขนาดเล็กสำหรับอัปเดต Attribute Values |
| `map_filter_patch` | Patch ขนาดเล็กสำหรับ Filter |
| `attribute_values` | Attribute Values หรือ Numeric Stats สำหรับ Selection/Edit UI |

### 6.4 กฎการสร้าง Attribute Style

**สำหรับ Attribute ประเภท String** → ใช้ MapLibre `match` expression

```json
["match", ["get", "re_nesdb"],
  "Northeast", "#FFFF00",
  "Central",   "#FFA500",
  "#00FFFF"
]
```

**สำหรับ Attribute ประเภท Numeric** → ใช้ MapLibre `interpolate` expression

```json
["interpolate", ["linear"], ["get", "dri_mean"],
  -8000, "#008000",
  -4000, "#FFA500",
   100,  "#0000FF"
]
```

---

## 7. Filter Flow

Filter แยกออกจาก Paint Style โดยสิ้นเชิง

**Event :** `map_filter_patch`

```
ผู้ใช้ขอ Filter
  → LLM/Tool Args มี filterConditions หรือ Filter Expression
  → Chatbot Resolve Active Layer และ Attribute Values
  → buildMapFilterPatch() สร้าง Patch
  → Frontend นำ Patch ไปใช้กับ Map Style ปัจจุบัน
  → Backend Sync Style ปัจจุบันพร้อม Filter เข้า conversation_map_layers.map_style
```

**ตัวอย่าง Filter Patch:**

```json
{
  "event": "map_filter_patch",
  "layerId": "66b4344b2ab4c9fe9eb2fa79",
  "operation": "set_filter",
  "patches": [
    {
      "layerType": "circle",
      "filter": ["==", ["get", "re_nesdb"], "Northeast"]
    }
  ]
}
```

**แหล่งข้อมูลสำหรับ Filter Matching (ตามลำดับ):**

1. Style Expressions ปัจจุบัน
2. Attribute Value Response
3. Layer Catalog Fields/Type
4. Active Layer Context


---

## 8. PMTiles Flow

PMTiles เป็น Render Transport อีกแบบสำหรับข้อมูล Layer เดิม

**ฟังก์ชัน:** `handleRenderPmtilesLayerTool()`

```
ผู้ใช้ขอดู Layer ปัจจุบันในรูปแบบ PMTiles
  → Backend ใช้ layer_catalog payload ปัจจุบัน
  → อ่าน pmtilesUrlTemplate จาก mapconfig.layerConfigTemplate
  → สร้าง Private Provider URL พร้อม API Key
  → ลบ Private api_key ออกก่อน Stream
  → Stream event: map / layer_catalog ที่มี layer.type = "pmtiles"
```

**ตัวอย่าง PMTiles Layer Payload:**

```json
{
  "event": "layer_catalog",
  "layer": {
    "type": "pmtiles",
    "renderType": "pmtiles",
    "layerId": "{id}",
    "title": "...",
    "url": "https://.../core/api/tiles/1.0-beta/pmtiles/{id}",
    "bounds": [],
    "minzoom": 0,
    "maxzoom": 14,
    "sourceLayer": "...",
    "geometryType": "point"
  }
}
```

> PMTiles เปลี่ยนการส่ง URL ออกไปใหม่ Tile Data 

---

## 9. State ปัจจุบัน, ประวัติ และ Undo

**โมเดล State :**

| แหล่งข้อมูล | บทบาท |
|---|---|
| `conversation_map_layers` | State ปัจจุบันสำหรับ Render/Edit |
| `messages.metadata` | ประวัติ Events และแหล่งข้อมูลสำหรับ Undo |
| Redis | Cache ชั่วคราวสำหรับ Chat/Map Selection |
| Memory/RAG | Context เท่านั้น ไม่ใช่ State ที่น่าเชื่อถือ |

### Undo Flow

```
Frontend ส่ง mapselection พร้อม Undo Key/LayerId
  → getMapStyleHistorySelection()
  → restoreConversationMapLayerStyleFromHistory()
  → อ่าน map_style เก่าจาก messages.metadata
  → อัปเดต conversation_map_layers.map_style
  → Stream map_style ที่ Restore แล้ว
```

