# การใช้งาน backend

## tech stack ที่ใช้
```text
Package Manager: Bun
Web Framework: ElysiaJS
ORM: Prisma
Database: Supabase
Security: JWT & Bcrypt
KMS: Infisical
```

##ขั้นตอนการ run backend 
1.clone project
```bash
git clone https://github.com/Nutiphongg/Intern_chatbotui.git
```
2.ติดตั้ง library ทั้งหมด
```bash
bun install
```
3.สร้างไฟล์ .env
```bash
DATABASE_URL="postgresql://postgres..."
ACCESS_SECRET="your_secret_key"
REFRESH_SECRET="your_refresh_key"
```
4.รัน ตัว prisma
```bash
bunx prisma db pull
bunx prisma generate
```
5..รัน project
```bash
bun run --watch src/index.ts
```
Open http://localhost:3000/swagger with your browser to see the result,


## Map + Chatbot Flow

เอกสารนี้อธิบาย flow การทำงานของ map tool ที่เชื่อมกับ chatbot โดยเน้นส่วนที่เกี่ยวกับ layer selection, style edit, attribute values, filter, และ PMTiles

## High Level Flow

```text
User prompt / mapselection
  -> chatbot route
  -> chatbot service decides tool call
  -> map tool reads provider config / layer config
  -> stream event back to frontend
  -> sync current layer state into conversation_map_layers
```

ไฟล์หลักที่เกี่ยวข้อง:

- `src/features/chatbot/service.ts`
  - orchestrate chat stream
  - decide selected layer from conversation state
  - call map tools
  - stream SSE events
  - sync `conversation_map_layers`

- `src/features/map/tools.ts`
  - execute map tools
  - build layer catalog
  - edit MapLibre style
  - fetch attribute values
  - build filter/style patches

## Conversation Map State

ระบบเก็บสถานะ map ปัจจุบันไว้ใน table `conversation_map_layers`

ข้อมูลสำคัญ:

- `conversation_id`
- `layer_key`
- `title`
- `type`
- `layer_payload`
- `map_style`
- `active_style`
- `visible`

หลักการใช้งาน:

- `layer_payload` คือข้อมูล layer catalog ที่ใช้ render map
- `map_style` คือ current style ล่าสุดของ layer นั้น
- `messages.metadata` ใช้เป็น history ของ event ที่เคยเกิดขึ้น
- `conversation_map_layers` ใช้เป็น current state สำหรับ render/edit ต่อ

## Conversation Memory And RAG

ระบบ chatbot สามารถใช้ memory/RAG เพื่อช่วยให้ model เข้าใจบริบทการคุยก่อนหน้า เช่น layer ที่ user เคยเลือก, style ที่เคยแต่ง, filter ที่เคยสั่ง, หรือ preference ของ user ใน conversation เดิม

หลักการทำงาน:

```text
messages / conversation summary
  -> embedding
  -> store in pgvector
  -> retrieve relevant memory by similarity
  -> inject as extra context for model
```

ค่าที่เกี่ยวข้องมักอยู่ใน `.env`:

```env
MEMORY_EMBEDDING_MODEL=nomic-embed-text
MEMORY_RAG_TOP_K=6
MEMORY_RAG_MATCH_THRESHOLD=0.2
MEMORY_SUMMARY_MODEL=qwen2.5:3b
```

หน้าที่ของแต่ละค่า:

- `MEMORY_EMBEDDING_MODEL` คือ model ที่ใช้แปลงข้อความเป็น vector สำหรับเก็บ/ค้นใน pgvector
- `MEMORY_RAG_TOP_K` คือจำนวน memory ที่จะดึงกลับมาเป็น context
- `MEMORY_RAG_MATCH_THRESHOLD` คือ threshold สำหรับกรอง memory ที่ไม่เกี่ยวข้อง
- `MEMORY_SUMMARY_MODEL` คือ model ที่ใช้สรุปบทสนทนาเพื่อเก็บเป็น memory

ลำดับความน่าเชื่อถือของข้อมูล:

1. `conversation_map_layers` คือ source of truth ของ current map state
2. `messages.metadata` คือ event history เช่น `layer_catalog`, `map_style`, `map_style_patch`, `map_filter_patch`, `attribute_values`
3. memory/RAG คือ context เสริมให้ model เข้าใจบทสนทนา ไม่ใช่ข้อมูลหลักสำหรับ render map

ถ้า memory/RAG ขัดกับ current state ใน `conversation_map_layers` ให้เชื่อ `conversation_map_layers` ก่อนเสมอ

ตัวอย่าง:

- ถ้า memory จำว่า user เคยแต่ง `re_royin` เป็นสีแดง แต่ `conversation_map_layers.map_style` ปัจจุบันเปลี่ยนเป็น `dri_mean` แล้ว ให้ใช้ style จาก `conversation_map_layers`
- ถ้า user พิมพ์ว่า `undo map style` ให้ดู history จาก `messages.metadata` เพื่อหา style ก่อนหน้า แล้ว update กลับเข้า `conversation_map_layers`
- ถ้า user ถามต่อว่า `เปลี่ยน North เป็นสีดำ` memory อาจช่วยเข้าใจว่าเคยคุยเรื่อง attribute region แต่ backend ยังต้องอิง active layer/style จาก `conversation_map_layers`

## Layer Selection

เวลา user สั่ง edit style หรือ filter ระบบต้องเลือกก่อนว่าจะทำกับ layer ไหน

Function หลัก:

- `selectMapLayerStateForEdit(...)`
- `selectMapLayerStateByText(...)`
- `scoreLayerTextMatch(...)`
- `getLayerMatchTerms(...)`
- `selectMapPayloadForEdit(...)`
- `selectMapStyleForEdit(...)`

ลำดับการเลือก layer:

1. ถ้ามี `layerId` จาก tool args หรือ `mapselection` ให้ใช้ layer นั้นก่อน
2. ถ้าไม่มี `layerId` ให้ match จากข้อความ เช่น title, sourceLayer, layer name
3. ถ้า match ไม่เจอ ให้ fallback ไป active/latest layer

เหตุผลที่ `layerId` สำคัญ:

ถ้า conversation มีหลาย layer และชื่อ layer คล้ายกัน เช่น title ภาษาไทยซ้ำบางส่วน การส่ง `layerId` มาจะเลือกได้แม่นกว่า title matching

ตัวอย่าง args ที่ควรส่ง:

```json
{
  "operation": "update_layer",
  "layerId": "66cd8af3ef0689bff43a5884",
  "attributeKey": "dri_mean"
}
```

## Map Options And Layer Catalog

การแสดง list layer ใช้ flow `map_options`

ตัวอย่าง prompt:

```text
show list vector tile
```

ถ้า user เลือก layer แล้ว backend จะส่ง event:

```text
event: map
data: {"event":"layer_catalog", ...}
```

ข้อมูล `layer_catalog.layer` จะถูก sync เข้า `conversation_map_layers.layer_payload`

## Style Edit Flow

Tool หลัก:

- `edit_map_style`

Function หลัก:

- `handleEditMapStyleTool(...)`
- `buildAttributeEditArgs(...)`
- `buildFilterEditArgsFromInstruction(...)`
- `buildFilterEditArgs(...)`

Flow:

```text
LLM calls edit_map_style
  -> chatbot selects target layer from conversation_map_layers
  -> build edit args
  -> map tool patches current map_style
  -> stream map_style / map_style_patch / map_filter_patch
  -> sync new style into conversation_map_layers.map_style
```

ตัวอย่าง prompt:

```text
style the map by attribute re_royin
change re_royin value North color to red
set circle-stroke-width to 4
remove circle-opacity
```

## Attribute Values Flow

เวลาแต่ง style ด้วย attribute เช่น `re_royin`, `pv_tn`, `dri_mean` backend ต้องมี value หรือ stats เพื่อสร้าง MapLibre expression

Function หลัก:

- `handleMapAttributeValuesTool(...)`
- `buildAttributeEditArgs(...)`
- `enrichMapSuggestionsWithAttributeValues(...)`

Config ที่มาจาก DB อยู่ใน layer config:

```json
{
  "attributeValues": {
    "connectionUrlTemplate": "/core/api/analytics/1.0/connections",
    "connectionQuery": {
      "type": "vallaris11"
    },
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

ขั้นตอน:

1. อ่าน `datasetId` จาก `layer_catalog.layer.attributes` หรือ payload ที่เกี่ยวข้อง
2. ยิง connection endpoint เพื่อหา `connectionId`
3. ยิง explore endpoint เพื่อหา values/stats
4. เอา values ไปสร้าง style expression หรือส่งออก `attribute_values`

ถ้า fetch value ไม่สำเร็จจาก network/timeout ควรแยก error เป็น data lookup error ไม่ควร fallback เป็น `No matching paint/layout property...`

## Filter Flow

Filter คือเงื่อนไขการ render feature ไม่ใช่ paint style

Event ที่ส่งออก:

```text
event: map_filter_patch
```

Operation ที่รองรับ:

- `set_filter`
- `add_filter`
- `remove_filter`
- `clear_filter`

ตัวอย่าง filter args:

```json
{
  "operation": "set_filter",
  "filterConditions": [
    {
      "attributeKey": "dri_mean",
      "operator": ">",
      "value": 50
    }
  ]
}
```

Numeric filter:

- ถ้า attribute type เป็น `Number`
- backend สามารถ parse operator และตัวเลขจาก prompt ได้
- เช่น `more than value is 50` จะใช้ active attribute จาก current `map_style.attributeStyleKey`

String filter:

- ใช้ value จาก `attribute_values`
- เช่น `show re_royin is North`

ตัวอย่าง prompt:

```text
filter dri_mean greater than 50
show only dri_mean > 50
filter layer dri_20240219 show more than value is 50
show re_royin is North
remove filter in map
```

## Style Switching

การเปลี่ยน style เช่น circle, heatmap, fill, 3D ต้องดูว่า style model เข้ากันได้หรือไม่

### Circle To Heatmap

หลักการ:

- ใช้ preset/knowledge ของ heatmap
- ไม่ย้าย paint/color/attribute expression จาก circle ไป heatmap
- preserve เฉพาะ `filter` เพราะ filter เป็นคนละส่วนกับ paint

เหตุผล:

`circle-color` และ `heatmap-color` มี semantics คนละแบบกัน จึงไม่ควรย้าย attribute color ข้ามกันแบบอัตโนมัติ

### Fill To 3D

หลักการ:

- สามารถ preserve attribute paint/filter ได้ ถ้า property และ style structure compatible
- ใช้ current map style เป็นฐาน แล้ว merge เข้ากับ preset ใหม่

## PMTiles Flow

PMTiles ใช้ layerId เดียวกับ vector tile ได้ แต่เปลี่ยน endpoint สำหรับ render source

Prompt ตัวอย่าง:

```text
show the map by pmtile
show current layer as pmtiles
```

Flow:

```text
User asks PMTiles
  -> backend uses latest/selected layer
  -> build PMTiles layer catalog
  -> stream event map
  -> frontend renders PMTiles URL
```

หมายเหตุ:

- ไม่ควรส่ง PMTiles URL ตั้งแต่ layer catalog ปกติ
- ส่ง PMTiles เฉพาะตอน user ขอ PMTiles
- ไม่ควร expose `api_key` ใน stream payload

## Stream Events

Event หลัก:

- `map_access`
```text
data: {"success":true,"allowedProviders":["VALLARIS"],"allowedHosts":[{"providerKey":"VALLARIS","hostKey":"dragonfly","baseUrl":"https://vallaris.dragonfly.gistda.or.th"}],"configs":[{"intentName":"show_vector_tile","type":"vector_tile","urlTemplate":"/core/api/features/1.0/collections"}]}
```
- `map_options`
```text
data: {"needInfo":true,"key":"layerId","choices":[{"label":"viirs 30 days (pipeline)","value":"66b2f5d6780755f5d43138b6","description":"ข้อมูลจุดความร้อนราย 30 วัน จาก Suomi NPP ระบบ VIIRS","type":"vector_tile","layerId":"66b2f5d6780755f5d43138b6","layerTitle":"viirs 30 days (pipeline)"},{"label":"viirs 1 day (pipeline)","value":"66b43475dcc3ef870b9c9f27","description":"ข้อมูลจุดความร้อนราย 1 วัน จาก Suomi NPP ระบบ VIIRS","type":"vector_tile","layerId":"66b43475dcc3ef870b9c9f27","layerTitle":"viirs 1 day (pipeline)"},{"label":"viirs 3 days (pipeline)","value":"66b4344b2ab4c9fe9eb2fa79","description":"ข้อมูลจุดความร้อนราย 3 วัน จาก Suomi NPP ระบบ VIIRS","type":"vector_tile","layerId":"66b4344b2ab4c9fe9eb2fa79","layerTitle":"viirs 3 days (pipeline)"},{"label":"viirs 7 days (pipeline)","value":"66b4345b2ab4c9fe9eb2fa7a","description":"ข้อมูลจุดความร้อนราย 7 วัน จาก Suomi NPP ระบบ VIIRS","type":"vector_tile","layerId":"66b4345b2ab4c9fe9eb2fa7a","layerTitle":"viirs 7 days (pipeline)"},{"label":"ขอบเขตการปกครอง (อำเภอ) 2559","value":"66b587aee223a86ac45a0f6e","type":"vector_tile","layerId":"66b587aee223a86ac45a0f6e","layerTitle":"ขอบเขตการปกครอง (อำเภอ) 2559"}],"selectedValues":{},"complete":false,"intentName":"show_vector_tile","provider":"vallaris","pagination":{"numberMatched":4347,"numberReturned":5,"hasNext":true,"hasBack":false},"question":"Please choose which layer."}

```
- `map`
```text
data: {"event":"layer_catalog","intentName":"show_vector_tile","provider":"vallaris","layer":{"type":"vector_tile","layerId":"66b4344b2ab4c9fe9eb2fa79","title":"viirs 3 days (pipeline)","url":"https://vallaris.dragonfly.gistda.or.th/core/api/tiles/1.0-beta/tiles/66b4344b2ab4c9fe9eb2fa79","tiles":["https://vallaris.dragonfly.gistda.or.th/core/api/tiles/1.0-beta/tiles/66b4344b2ab4c9fe9eb2fa79/%7Bz%7D/%7Bx%7D/%7By%7D"],"minzoom":3,"maxzoom":10,"bounds":[88.3482499,-10.16153,140.93004,30.99824],"geometryType":"point","sourceLayer":"viirs 3 days (pipeline)"}}
```
- `map_style`
```text
data: {"success":true,"event":"map_style","layerId":"66b4344b2ab4c9fe9eb2fa79","geometryType":"point","styleKey":"circle","styleName":"Single Points","activeStyle":"circle","defaultStyle":"circle","layers":[{"type":"circle","paint":{"circle-color":["interpolate",["linear"],["get","bright_ti5"],290,"#FFFF00",315,"#FFA500",340,"#FF0000"],"circle-radius":["interpolate",["linear"],["get","bright_ti5"],290,5,340,14],"circle-stroke-width":1.5,"circle-stroke-color":"#ffffff","circle-opacity":0.9}}]}
```
- `map_style_patch`
```text
data: {"event":"map_style_patch","layerId":"66b4344b2ab4c9fe9eb2fa79","attributeKey":"re_nesdb","paintKey":"circle-color","outputType":"color","operation":"update_stops","patches":[{"attributeValue":"North","output":"#FFA500"},{"attributeValue":"East","output":"#FFC0CB"}]}

```
- `map_filter_patch`
```text
data: {"event":"map_filter_patch","layerId":"66b4344b2ab4c9fe9eb2fa79","operation":"set_filter","patches":[{"layerType":"circle","filter":["all",["==",["get","re_nesdb"],"North"],["==",["get","re_nesdb"],"East"]]}]}

```
- `attribute_values`
```text
data: {"layerId":"66b4344b2ab4c9fe9eb2fa79","attributeKey":"re_nesdb","attributeType":"String","values":["UpperNorth","Northeast","Central","West","LowwerNorth","South"],"numberMatched":7}
```
- `suggestions`
```text
data: {"items":[{"key":"change_style","label":"style to ","value":"heatmap","promptTemplate":"Change the current map layer style to {value} "},{"key":"change_color","label":"color style to ","value":"blue","promptTemplate":"Change the current map layer primary color to {value}"},{"key":"style_by_attribute","label":"Add attribute for map style","promptTemplate":"edit style the map by attribute "},{"key":"clear_layer","label":"Clear layer","promptTemplate":"Clear map layer "},{"key":"clear_all_layers","label":"Clear all layers","promptTemplate":"Clear all map layers"}]}
```
- `map_error`
```text
data: {"message":"missing_x_api_key","needsApiKey":true,"apiKeyHeader":"X-API-Key","silentRetrySupported":true}
```

### `map`

ใช้ส่ง layer catalog สำหรับ render layer

### `map_style`

ใช้ส่ง full style ที่ frontend ควร render

### `map_style_patch`

ใช้ส่ง patch เฉพาะ value/style stops ที่แก้ เพื่อให้ frontend update current style ได้

### `map_filter_patch`

ใช้ส่ง filter patch เฉพาะ layer

### `attribute_values`

ใช้ส่ง value ของ attribute ที่ user เลือกไว้ เพื่อให้ frontend แสดงตัวเลือกหรือใช้ต่อในการแก้ style

## Error Cases

ข้อความนี้:

```text
No matching paint/layout property was changed for this style edit request.
```

เกิดเมื่อ:

- tool ถูกเรียกเป็น `edit_map_style`
- ไม่ใช่ filter operation
- patch แล้ว `layers` ไม่เปลี่ยนจากของเดิม
- ไม่เข้า case attribute values ว่าง

สาเหตุที่พบบ่อย:

- เลือก target layer ผิด
- paintKey ไม่ตรงกับ current style
- attribute value edit แต่ current style ยังไม่มี expression ของ attribute นั้น
- backend ไม่มี values/stats ที่ต้องใช้สร้าง style expression
- network error ตอน fetch attribute values

ควรปรับเพิ่มในอนาคต:

- แยก error code เช่น `NO_ATTRIBUTE_VALUES`, `NO_MATCHING_PAINT_KEY`, `TARGET_LAYER_NOT_FOUND`, `ATTRIBUTE_VALUES_CONNECTION_FAILED`
- retry เฉพาะ network/timeout ตอน fetch attribute values
- ถ้า error deterministic ให้ stream `map_error` แล้วจบ ไม่ให้ LLM แต่งคำตอบยาวเอง

## Test Prompts

แสดง list layer:

```text
show list vector tile
```

เลือก layer:

```json
{
  "mapselection": {
    "key": "layerId",
    "value": "66b43475dcc3ef870b9c9f27"
  }
}
```

แต่งด้วย attribute:

```text
style the map by attribute re_royin
style layer viirs 7 days by attribute re_royin
style layer 66b43475dcc3ef870b9c9f27 by attribute re_royin
```

แก้สี value:

```text
change re_royin value North color to red
change re_royin value North circle-color to red and West to blue
```

Filter:

```text
show re_royin is North
filter dri_mean greater than 50
filter layer dri_20240219 show more than value is 50
remove filter in map
```

เปลี่ยน style:

```text
change layer style to heatmap
change layer style to circle
change layer style to 3d_extrusion
```

PMTiles:

```text
show the map by pmtile
show current layer as pmtiles
```

## Recommended Frontend Contract

เวลามีหลาย layer ใน conversation ควรส่ง `layerId` กลับมาให้ backend ทุกครั้งที่ user เลือก layer จาก UI/chip

ตัวอย่าง:

```json
{
  "message": "style the map by attribute re_royin",
  "mapselection": {
    "key": "layerId",
    "value": "66b43475dcc3ef870b9c9f27"
  }
}
```

ถ้ามี `layerId` backend จะไม่ต้องเดาจาก title และจะใช้ `layer_payload` / `map_style` ของ layer นั้นจาก `conversation_map_layers` ได้ตรงที่สุด

