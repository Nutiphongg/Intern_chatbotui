import { HttpError } from "./problem";



export const Errors = {
  userAlreadyExists: () =>
    new HttpError(
      'Email or username already exists in the system',
      400,
      'user-already-exists'
    ),
  invalidCredentials: () =>
    new HttpError(
      'Email or password is incorrect',
      401,
      'invalid-credentials'
    ),

  missingToken: () =>
    new HttpError(
      'Token not found',
      401,
      'missing-token'
    ),

  invalidToken: () =>
    new HttpError(
      'Token is invalid or expired',
      401,
      'invalid-token'
    ),

  sessionNotFound: () =>
    new HttpError(
      'Session has expired or been cancelled',
      401,
      'session-not-found'
    ),

  badRequest: (message = 'Request is invalid') =>
    new HttpError(
      message,
      400,
      'bad-request'
    ),

  forbidden: (message = 'No permission to access this data') =>
    new HttpError(
      message,
      403,
      'forbidden'
    ),

  internalServerError: () =>
    new HttpError(
      'An internal system error occurred',
      500,
      'internal-server-error'
    )
}