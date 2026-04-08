export class HttpError extends Error {
  status: number
  type?: string

  constructor(message: string, status: number, type?: string) {
    super(message)
    this.status = status
    this.type = type
  }
}

export const problem = ({
  type,
  title,
  status,
  detail,
  instance
}: {
  type?: string
  title: string
  status: number
  detail?: string
  instance?: string
}) => ({
  type: type || 'about:blank',
  title,
  status,
  detail,
  instance
})