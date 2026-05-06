import { prisma } from "../setup/prisma";
import { decrypt, hashApiKey } from "../setup/encryption";

type MapToolArgs = {
  intentName?: string;
  provider?: string;
  params?: unknown;
  options?: unknown;
  selectedOptions?: unknown;
  variables?: unknown;
  [key: string]: unknown;
};

type ResolvedUserApiKey = {
  id: string;
  provider: string;
  keyName: string;
  encryptedKey: string;
  iv: string;
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
};

const pickRecord = (value: unknown): Record<string, unknown> => {
  return isRecord(value) ? value : {};
};

const replaceTemplateVariables = (
  template: string,
  variables: Record<string, unknown>
): string => {
  return Object.entries(variables).reduce((result, [key, value]) => {
    if (value === undefined || value === null) return result;
    return result.replace(new RegExp(`{${key}}`, "g"), String(value));
  }, template);
};

const hasUnresolvedTemplate = (value: string): boolean => /{[^}]+}/.test(value);

const withoutSensitiveValues = (variables: Record<string, unknown>) => {
  const { apiKey: _apiKey, ...safeVariables } = variables;
  return safeVariables;
};

const decryptUserApiKey = (apiKey: ResolvedUserApiKey): string => {
  return decrypt(apiKey.encryptedKey, apiKey.iv);
};

const resolveUserMapApiKeys = async (
  userId: string,
  headerApiKey?: string
): Promise<ResolvedUserApiKey[]> => {
  const cleanHeaderApiKey = headerApiKey?.trim();

  if (cleanHeaderApiKey) {
    return prisma.user_apikey.findMany({
      where: {
        userId,
        keyHash: hashApiKey(cleanHeaderApiKey),
        isActive: true
      },
      select: {
        id: true,
        provider: true,
        keyName: true,
        encryptedKey: true,
        iv: true
      }
    });
  }

  return prisma.user_apikey.findMany({
    where: {
      userId,
      isActive: true
    },
    select: {
      id: true,
      provider: true,
      keyName: true,
      encryptedKey: true,
      iv: true
    },
    orderBy: { createdAt: "desc" }
  });
};

const buildTemplateVariables = (
  aiArgs: MapToolArgs,
  layerConfigTemplate: unknown,
  intentName: string,
  provider: string,
  apiKey: string
): Record<string, unknown> => {
  const configTemplate = pickRecord(layerConfigTemplate);
  const reservedKeys = new Set([
    "intentName",
    "provider",
    "params",
    "options",
    "selectedOptions",
    "variables"
  ]);

  const directArgs = Object.fromEntries(
    Object.entries(aiArgs).filter(([key]) => !reservedKeys.has(key))
  );

  return {
    ...pickRecord(configTemplate.defaults),
    ...pickRecord(configTemplate.params),
    ...directArgs,
    ...pickRecord(aiArgs.params),
    ...pickRecord(aiArgs.options),
    ...pickRecord(aiArgs.selectedOptions),
    ...pickRecord(aiArgs.variables),
    intentName,
    provider,
    apiKey
  };
};

export const mapToolSchema = {
  type: "function",
  function: {
    name: "get_map_layer",
    description: "สร้าง URL และ layer payload จาก mapconfig กลางและ option values ที่เลือกแล้ว",
    parameters: {
      type: "object",
      properties: {
        intentName: {
          type: "string",
          description: "ชื่อ intent จาก mapconfig ที่ได้จาก map_access หรือ option_info"
        },
        provider: {
          type: "string",
          enum: ["GISTDA", "VALLARIS"],
          description: "provider จาก map_access ที่ user มีสิทธิ์ใช้งาน"
        },
        params: {
          type: "object",
          description: "ค่าที่เลือกจาก option_info สำหรับแทน placeholder ใน urlTemplate/layerConfigTemplate"
        },
        options: {
          type: "object",
          description: "alias ของ params สำหรับค่าที่ user เลือก"
        },
        variables: {
          type: "object",
          description: "ตัวแปรเพิ่มเติมสำหรับแทน placeholder ใน template"
        }
      },
      required: ["intentName", "provider"]
    }
  }
};

export const handleMapTool = async (
  userId: string,
  aiArgs: MapToolArgs,
  headerApiKey?: string
) => {
  try {
    const intentName = aiArgs.intentName?.trim();
    const provider = aiArgs.provider?.trim();

    if (!intentName || !provider) {
      return { error: "AI ส่งข้อมูล map tool ไม่ครบ ต้องมี intentName และ provider ครับ" };
    }

    const config = await prisma.mapconfig.findUnique({
      where: {
        intentName_provider: {
          intentName,
          provider
        }
      }
    });

    if (!config) {
      return {
        error: `ไม่พบ mapconfig สำหรับคำสั่ง ${intentName} ของผู้ให้บริการ ${provider} ครับ`
      };
    }

    if (!config.isActive) {
      return {
        error: `mapconfig ${config.intentName} ของ ${config.provider} ถูกปิดใช้งานครับ`
      };
    }

    const userApiKey = (await resolveUserMapApiKeys(userId, headerApiKey))
      .find((apiKey) => apiKey.provider === config.provider);

    if (!userApiKey) {
      return {
        error: headerApiKey?.trim()
          ? `API Key ที่ส่งมาทาง header ไม่ตรงกับ provider ${config.provider} หรือไม่มีสิทธิ์ใช้งานครับ`
          : `ผู้ใช้ยังไม่ได้ผูก API Key สำหรับ ${config.provider} กรุณาตั้งค่า API Key ก่อนครับ`
      };
    }

    let decryptedApiKey = "";
    try {
      decryptedApiKey = decryptUserApiKey(userApiKey);
    } catch (error) {
      console.error("Decrypt map API key error:", error);
      return { error: "เกิดข้อผิดพลาดในการอ่าน API Key ของคุณ อาจมีการตั้งค่าผิดพลาด" };
    }

    const templateVariables = buildTemplateVariables(
      aiArgs,
      config.layerConfigTemplate,
      intentName,
      provider,
      decryptedApiKey
    );

    const finalUrl = replaceTemplateVariables(
      `${config.baseUrl}${config.urlTemplate}`,
      templateVariables
    );

    const configString = replaceTemplateVariables(
      config.layerConfigTemplate ? JSON.stringify(config.layerConfigTemplate) : "{}",
      templateVariables
    );

    if (hasUnresolvedTemplate(finalUrl) || hasUnresolvedTemplate(configString)) {
      return {
        error: `ข้อมูลสำหรับสร้าง URL แผนที่ยังไม่ครบครับ URL/template ยังมีตัวแปรที่ไม่ได้แทนค่า: ${finalUrl}`
      };
    }

    let finalLayerConfig: Record<string, unknown>;
    try {
      finalLayerConfig = JSON.parse(configString);
    } catch (error) {
      console.error("Map layerConfigTemplate parse error:", error);
      return { error: "layerConfigTemplate ของ mapconfig ไม่ใช่ JSON ที่ใช้งานได้หลังแทนค่าครับ" };
    }

    return {
      success: true,
      payload: {
        event: "layer_catalog",
        layer: {
          intentName,
          provider,
          ...finalLayerConfig,
          url: finalUrl,
          variables: withoutSensitiveValues(templateVariables)
        }
      }
    };
  } catch (error) {
    console.error("Map Tool Handler Error:", error);
    return { error: "ระบบฐานข้อมูลแผนที่ขัดข้องชั่วคราวครับ" };
  }
};

export const checkMapAccessSchema = {
  type: "function",
  function: {
    name: "check_user_map",
    description: "ตรวจสอบ provider API keys ของ user และดึง mapconfig กลางที่ user มีสิทธิ์ใช้งาน",
    parameters: {
      type: "object",
      properties: {},
      required: []
    }
  }
};

export const handleCheckMapAccess = async (userId: string, headerApiKey?: string) => {
  try {
    const userKeys = await resolveUserMapApiKeys(userId, headerApiKey);

    if (userKeys.length === 0) {
      return {
        success: false,
        allowedProviders: [],
        configs: [],
        message: headerApiKey?.trim()
          ? "The x-api-key header does not match any active API key for this user."
          : "The user has not linked any map API keys. Please advise the user to configure their API keys in the settings."
      };
    }

    const allowedProviders = Array.from(new Set(userKeys.map((key) => key.provider)));

    const configs = await prisma.mapconfig.findMany({
      where: {
        provider: {
          in: allowedProviders
        },
        isActive: true
      },
      select: {
        intentName: true,
        provider: true,
        baseUrl: true,
        urlTemplate: true,
        layerConfigTemplate: true
      },
      orderBy: [
        { provider: "asc" },
        { intentName: "asc" }
      ]
    });

    if (configs.length === 0) {
      return {
        success: false,
        allowedProviders,
        configs: [],
        message: "The user has map API keys, but no active mapconfig exists for those providers."
      };
    }

    return {
      success: true,
      allowedProviders,
      configs
    };
  } catch (error) {
    console.error("Check Map Access Error:", error);
    return {
      success: false,
      allowedProviders: [],
      configs: [],
      message: "An error occurred while retrieving map access permissions from the database."
    };
  }
};
