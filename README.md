# การใช้งาน backend

## tech stack ที่ใช้
Package Manager: Bun
Web Framework: ElysiaJS
ORM: Prisma
Database: Supabase
Security: JWT & Bcrypt

##ขั้นตอนการ run backend 
1.
```bash

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
4.รัน project
```bash
bun run --watch src/index.ts
```


Open http://localhost:3000/swagger with your browser to see the result.
