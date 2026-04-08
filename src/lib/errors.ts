import { HttpError } from "./problem";

export const Errors = {
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
    )
}