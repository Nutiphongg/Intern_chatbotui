export interface AuthUser {
  id: string;
  email: string;
  username: string;
}

export interface DeviceInfo {
  user_agent: string;
}

export interface JwtPayloadShape {
  userId: string;
}

export interface LoginServiceResult {
  accessToken: string;
  refreshToken: string;
  user: AuthUser;
}

export interface RefreshServiceResult {
  newAccessToken: string;
  newRefreshToken: string;
}

export interface ActiveDevicesResult {
  user: number;
  devices: DeviceInfo[];
}

export interface LogoutServiceResult {
  message: string;
}

