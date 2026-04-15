import { HttpError } from "./problem";



export const Errors = {
  userAlreadyExists: () =>
    new HttpError(
      'email หรือ username นี้มีในระบบแล้ว',
      400,
      'user-already-exists'
    ),
  invalidCredentials: () =>
    new HttpError(
      'Email หรือ password ไม่ถูกต้อง',
      401,
      'invalid-credentials'
    ),

  missingToken: () =>
    new HttpError(
      'ไม่พบ token',
      401,
      'missing-token'
    ),

  invalidToken: () =>
    new HttpError(
      'Token ไม่ถูกต้องหรือหมดอายุ',
      401,
      'invalid-token'
    ),

  sessionNotFound: () =>
    new HttpError(
      'Session หมดอายุหรือถูกยกเลิก',
      401,
      'session-not-found'
    ),

  badRequest: (message = 'คำขอไม่ถูกต้อง') =>
    new HttpError(
      message,
      400,
      'bad-request'
    ),

  forbidden: (message = 'ไม่มีสิทธิ์เข้าถึงข้อมูลนี้') =>
    new HttpError(
      message,
      403,
      'forbidden'
    ),

  internalServerError: () =>
    new HttpError(
      'เกิดข้อผิดพลาดภายในระบบ',
      500,
      'internal-server-error'
    )
}