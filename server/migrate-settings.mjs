/**
 * migrate-settings.mjs
 *
 * 将 Node.js 后端的配置文件 (_appdata/settings.js + _appdata/mysettings.json)
 * 合并生成 .NET 后端的 appsettings.json
 *
 * 用法:
 *   node migrate-settings.mjs [appdata目录路径] [输出目录路径]
 *
 * 默认:
 *   appdata 目录: server/_appdata
 *   输出路径:     server-dotnet/Server/appsettings.json
 */

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ─── Resolve paths ───────────────────────────────────────────────
const appDataDir = process.argv[2]
    ? resolve(process.argv[2])
    : resolve(__dirname, '_appdata');

const outputDir = process.argv[3]
    ? resolve(process.argv[3])
    : resolve(__dirname, '..', 'server-dotnet', 'Server');

const settingsJsPath = resolve(appDataDir, 'settings.js');
const mySettingsPath = resolve(appDataDir, 'mysettings.json');
const outputPath = resolve(outputDir, 'appsettings.json');

console.log('=== FUXA Settings Migration ===');
console.log(`AppData dir:  ${appDataDir}`);
console.log(`settings.js:  ${settingsJsPath}`);
console.log(`mysettings:   ${mySettingsPath}`);
console.log(`Output:       ${outputPath}`);
console.log('');

// ─── 1. Parse settings.js (CommonJS module) ─────────────────────

function parseSettingsJs(filePath) {
    if (!existsSync(filePath)) {
        console.log('[WARN] settings.js not found, skipping.');
        return {};
    }

    let content = readFileSync(filePath, 'utf8');

    // Remove module.exports = { ... } wrapper → just the object literal
    content = content.replace(/module\.exports\s*=\s*\{/, '{');
    content = content.replace(/\}\s*;?\s*$/, '}');

    // Replace process.env.XXX || <default> with <default>
    content = content.replace(/process\.env\.\w+\s*\|\|\s*/g, '');

    // Remove single-line comments (// ...)
    content = content.replace(/\/\/[^\n]*/g, '');

    // Remove multi-line comments (/* ... */)
    content = content.replace(/\/\*[\s\S]*?\*\//g, '');

    // Convert unquoted property keys to quoted: key: → "key":
    content = content.replace(/([{,])\s*([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:/g, '$1"$2":');

    // Convert single-quoted string values to double-quoted
    content = content.replace(/:\s*'([^']*)'/g, ':"$1"');

    // Remove trailing commas before } or ] (not valid in JSON)
    content = content.replace(/,(\s*[}\]])/g, '$1');

    try {
        return JSON.parse(content);
    } catch (err) {
        console.error('[ERROR] Failed to parse settings.js:', err.message);
        console.error('Intermediate JSON:\n', content);
        process.exit(1);
    }
}

// ─── 2. Parse mysettings.json ───────────────────────────────────

function parseMySettings(filePath) {
    if (!existsSync(filePath)) {
        console.log('[WARN] mysettings.json not found, skipping.');
        return {};
    }

    try {
        return JSON.parse(readFileSync(filePath, 'utf8'));
    } catch (err) {
        console.error('[ERROR] Failed to parse mysettings.json:', err.message);
        process.exit(1);
    }
}

// ─── 3. Deep merge utility ──────────────────────────────────────

function deepMerge(target, source) {
    const result = { ...target };
    for (const key of Object.keys(source)) {
        if (source[key] === undefined) continue;
        if (
            source[key] !== null &&
            typeof source[key] === 'object' &&
            !Array.isArray(source[key]) &&
            typeof target[key] === 'object' &&
            target[key] !== null
        ) {
            result[key] = deepMerge(target[key], source[key]);
        } else {
            result[key] = source[key];
        }
    }
    return result;
}

// ─── 4. Build connection string ─────────────────────────────────

function buildConnectionString(dbType, config) {
    if (!config || typeof config !== 'object') return '';

    switch (dbType) {
        case 'mssql': {
            const parts = [
                `Server=${config.host || 'localhost'},${config.port || 1433}`,
                `Database=${config.database || 'fuxa'}`,
                `User Id=${config.user || 'sa'}`,
                `Password=${config.password || ''}`,
                `TrustServerCertificate=${config.trustServerCertificate !== false ? 'true' : 'false'}`,
            ];
            if (config.poolMax) {
                parts.push(`Max Pool Size=${config.poolMax}`);
            }
            return parts.join(';') + ';';
        }
        case 'mysql': {
            const parts = [
                `server=${config.host || 'localhost'}`,
                `port=${config.port || 3306}`,
                `Database=${config.database || 'fuxa'}`,
                `Uid=${config.user || 'root'}`,
                `Pwd=${config.password || ''}`,
                `CharSet=utf8mb4`,
            ];
            if (config.poolMax) {
                parts.push(`MaxPoolSize=${config.poolMax}`);
            }
            return parts.join(';') + ';';
        }
        default:
            return '';
    }
}

// ─── 5. Convert Node.js settings to .NET appsettings.json ───────

function toAppSettings(merged) {
    const dbType = merged.dbType || 'sqlite';
    const dbConfig = dbType === 'mssql' ? merged.mssql : merged.mysql;

    const fuxa = {
        Version: merged.version ?? 1.4,
        Language: merged.language ?? 'en',
        HideEditorOnboarding: merged.hideEditorOnboarding ?? false,
        UiPort: merged.uiPort ?? 1881,
        LogApiLevel: merged.logApiLevel ?? 'tiny',
        DaqEnabled: merged.daqEnabled ?? true,
        DaqTokenizer: merged.daqTokenizer ?? 24,
        Logs: {
            Retention: merged.logs?.retention ?? 'none',
        },
        BroadcastAll: merged.broadcastAll ?? false,
        AllowedOrigins: merged.allowedOrigins ?? [
            'http://localhost',
            'http://127.0.0.1',
            'http://192.168.*',
            'http://10.*',
            'http://localhost:4200',
        ],
        SecureEnabled: merged.secureEnabled ?? false,
        TokenExpiresIn: merged.tokenExpiresIn ?? '1h',
        EnableRefreshCookieAuth: merged.enableRefreshCookieAuth ?? false,
        RefreshTokenExpiresIn: merged.refreshTokenExpiresIn ?? '7d',
        SecureOnlyEditor: merged.secureOnlyEditor ?? false,
        SecretCode: merged.secretCode ?? '',
        HeartbeatIntervalSec: merged.heartbeatIntervalSec ?? 10,
        WebcamSnapShotsCleanup: merged.webcamSnapShotsCleanup ?? false,
        WebcamSnapShotsRetain: merged.webcamSnapShotsRetain ?? 7,
        SwaggerEnabled: merged.swaggerEnabled ?? false,
        NodeRedEnabled: merged.nodeRedEnabled ?? false,
        NodeRedAuthMode: merged.nodeRedAuthMode ?? 'secure',
        NodeRedUnsafeModules: merged.nodeRedUnsafeModules ?? false,
        LogFull: merged.logFull ?? false,
        UserRole: merged.userRole ?? false,
        Stmp: {
            Host: merged.smtp?.host ?? '',
            Port: merged.smtp?.port ?? 587,
            Mailsender: merged.smtp?.mailsender ?? '',
            Username: merged.smtp?.username ?? '',
            Password: merged.smtp?.password ?? '',
        },
        Alarms: {
            Retention: merged.alarms?.retention ?? 'year1',
            RetentionType: merged.alarms?.retentionType ?? 'days',
            RetentionDays: merged.alarms?.retentionDays ?? 365,
        },
        DaqStore: {
            Type: merged.daqstore?.type ?? 'SQlite',
            Url: merged.daqstore?.url ?? '',
            Organization: merged.daqstore?.organization ?? '',
            Database: merged.daqstore?.tableName ?? merged.daqstore?.database ?? '',
            Retention: merged.daqstore?.retention ?? 'year1',
            Credentials: {
                Token: merged.daqstore?.credentials?.token ?? merged.daqstore?.configurationString ?? '',
                UserName: merged.daqstore?.credentials?.userName ?? '',
                Password: merged.daqstore?.credentials?.password ?? '',
            },
        },
        Database: {
            Type: dbType,
            ConnectionString: buildConnectionString(dbType, dbConfig),
        },
    };

    return { fuxa };
}

// ─── 6. Main ────────────────────────────────────────────────────

const settingsJs = parseSettingsJs(settingsJsPath);
const mySettings = parseMySettings(mySettingsPath);

console.log('[INFO] settings.js keys:', Object.keys(settingsJs).join(', '));
console.log('[INFO] mysettings.json keys:', Object.keys(mySettings).join(', '));

// mysettings.json overrides settings.js
const merged = deepMerge(settingsJs, mySettings);
console.log('[INFO] Merged keys:', Object.keys(merged).join(', '));

const { fuxa } = toAppSettings(merged);

// Read existing appsettings.json to preserve non-Fuxa sections
let existingRoot = {};
if (existsSync(outputPath)) {
    try {
        // Strip BOM if present
        const raw = readFileSync(outputPath, 'utf8').replace(/^\uFEFF/, '');
        existingRoot = JSON.parse(raw);
        console.log('[INFO] Preserving existing top-level keys:', Object.keys(existingRoot).join(', '));
    } catch (e) {
        console.log('[WARN] Could not read existing appsettings.json:', e.message);
    }
}

// Ensure standard ASP.NET Core sections exist
if (!existingRoot.Logging) {
    existingRoot.Logging = {
        LogLevel: {
            Default: 'Information',
            'Microsoft.AspNetCore': 'Warning',
        },
    };
}
if (!existingRoot.AllowedHosts) {
    existingRoot.AllowedHosts = '*';
}

const output = {
    Logging: existingRoot.Logging,
    AllowedHosts: existingRoot.AllowedHosts,
    ...Object.fromEntries(Object.entries(existingRoot).filter(([k]) => k !== 'Logging' && k !== 'AllowedHosts' && k !== 'Fuxa')),
    Fuxa: fuxa,
};

writeFileSync(outputPath, JSON.stringify(output, null, 2) + '\n', 'utf8');

console.log('');
console.log('[OK] appsettings.json generated successfully!');
console.log(`     Path: ${outputPath}`);
console.log('');
console.log('--- Fuxa section preview ---');
console.log(JSON.stringify(fuxa, null, 2));
