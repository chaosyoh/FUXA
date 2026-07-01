#!/usr/bin/env node
/**
 * FUXA API Comparison Script
 * 同时调用 Node.js server (1881) 和 .NET server-dotnet (1882) 的 API 接口，
 * 比较返回结果是否一致。
 *
 * 用法:
 *   node compare-api.mjs                             # 默认只测读操作
 *   node compare-api.mjs --write                     # 包含安全写入操作
 *   node compare-api.mjs --user admin --pass 123456  # 自定义登录凭据
 *   node compare-api.mjs --node-port 1881 --dotnet-port 1882
 */

// ─── 配置 ────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

function getArg(name, defaultVal) {
    const idx = args.indexOf(`--${name}`);
    if (idx !== -1 && args[idx + 1]) return args[idx + 1];
    return defaultVal;
}

const NODE_PORT   = getArg('node-port', '1881');
const DOTNET_PORT = getArg('dotnet-port', '1882');
const NODE_BASE   = `http://127.0.0.1:${NODE_PORT}`;
const DOTNET_BASE = `http://127.0.0.1:${DOTNET_PORT}`;
const USERNAME    = getArg('user', 'admin');
const PASSWORD    = getArg('pass', '123456');
const INCLUDE_WRITE = args.includes('--write');

// ─── 颜色 ────────────────────────────────────────────────────────────────────

const C = {
    reset:  '\x1b[0m',
    bold:   '\x1b[1m',
    dim:    '\x1b[2m',
    green:  '\x1b[32m',
    red:    '\x1b[31m',
    yellow: '\x1b[33m',
    cyan:   '\x1b[36m',
};

function badge(status) {
    const map = { PASS: C.green, DIFF: C.yellow, FAIL: C.red, SKIP: C.dim };
    const color = map[status] || C.reset;
    return `${color}${status}${C.reset}`;
}

// ─── HTTP 工具 ───────────────────────────────────────────────────────────────

async function fetchSafe(url, options = {}) {
    try {
        const res = await fetch(url, { redirect: 'follow', ...options });
        const ct  = res.headers.get('content-type') || '';
        let body;
        if (ct.includes('json')) {
            body = await res.json().catch(() => null);
        } else {
            body = await res.text();
        }
        return { status: res.status, body, error: null };
    } catch (err) {
        return { status: 0, body: null, error: err.message };
    }
}

function buildOptions(method, body, token) {
    const headers = { 'Content-Type': 'application/json' };
    if (token) headers['x-access-token'] = token;
    const opts = { method, headers };
    if (body !== undefined) opts.body = JSON.stringify(body);
    return opts;
}

// ─── 深度对比 ────────────────────────────────────────────────────────────────

function deepDiff(a, b, path = '') {
    const diffs = [];
    if (a === b) return diffs;

    const type_a = Array.isArray(a) ? 'array' : typeof a;
    const type_b = Array.isArray(b) ? 'array' : typeof b;

    if (a === null || b === null || type_a !== type_b) {
        diffs.push({ path: path || '(root)', nodeVal: a, dotnetVal: b });
        return diffs;
    }
    if (type_a === 'array') {
        if (a.length !== b.length) {
            diffs.push({ path: `${path || '(root)'}.length`, nodeVal: a.length, dotnetVal: b.length });
        }
        const len = Math.min(a.length, b.length);
        for (let i = 0; i < len; i++) {
            diffs.push(...deepDiff(a[i], b[i], `${path}[${i}]`));
        }
        return diffs;
    }
    if (type_a === 'object') {
        const allKeys = new Set([...Object.keys(a), ...Object.keys(b)]);
        for (const key of allKeys) {
            const cp = path ? `${path}.${key}` : key;
            if (!(key in a)) {
                diffs.push({ path: cp, nodeVal: undefined, dotnetVal: b[key] });
            } else if (!(key in b)) {
                diffs.push({ path: cp, nodeVal: a[key], dotnetVal: undefined });
            } else {
                diffs.push(...deepDiff(a[key], b[key], cp));
            }
        }
        return diffs;
    }
    if (a !== b) {
        diffs.push({ path: path || '(root)', nodeVal: a, dotnetVal: b });
    }
    return diffs;
}

function truncate(val, maxLen = 80) {
    if (val === undefined) return C.dim + 'undefined' + C.reset;
    const s = JSON.stringify(val) ?? 'null';
    return s.length > maxLen ? s.slice(0, maxLen - 3) + '...' : s;
}

// ─── 测试用例 ─────────────────────────────────────────────────────────────────

const NOW = Date.now();

// phase 1 = public, phase 3 = auth-required reads, phase 4 = write (--write flag)
const CASES = [
    // ── Phase 1: Public ──────────────────────────────────────────────────────
    { phase: 1, name: 'GET  /api/version',
      method: 'GET', path: '/api/version' },

    { phase: 1, name: 'GET  /api/settings',
      method: 'GET', path: '/api/settings' },

    // ── Phase 3: Authenticated reads ─────────────────────────────────────────
    { phase: 3, name: 'GET  /api/project',
      method: 'GET', path: '/api/project' },

    { phase: 3, name: 'GET  /api/projectVersion',
      method: 'GET', path: '/api/projectVersion' },

    { phase: 3, name: 'GET  /api/projectdemo',
      method: 'GET', path: '/api/projectdemo' },

    { phase: 3, name: 'GET  /api/getDevices',
      method: 'GET', path: '/api/getDevices' },

    { phase: 3, name: 'POST /api/getTagValues',
      method: 'POST', path: '/api/getTagValues', body: [] },

    { phase: 3, name: 'GET  /api/users',
      method: 'GET', path: '/api/users' },

    { phase: 3, name: 'GET  /api/roles',
      method: 'GET', path: '/api/roles' },

    { phase: 3, name: 'GET  /api/apikeys',
      method: 'GET', path: '/api/apikeys' },

    { phase: 3, name: 'GET  /api/alarms',
      method: 'GET', path: '/api/alarms' },

    { phase: 3, name: 'GET  /api/alarmsHistory',
      method: 'GET', path: '/api/alarmsHistory',
      query: { start: NOW - 86400000, end: NOW } },

    { phase: 3, name: 'GET  /api/getAlarms',
      method: 'GET', path: '/api/getAlarms' },

    { phase: 3, name: 'GET  /api/resources/images',
      method: 'GET', path: '/api/resources/images' },

    { phase: 3, name: 'GET  /api/resources/resources',
      method: 'GET', path: '/api/resources/resources' },

    { phase: 3, name: 'GET  /api/resources/widgets',
      method: 'GET', path: '/api/resources/widgets' },

    { phase: 3, name: 'GET  /api/resources/templates',
      method: 'GET', path: '/api/resources/templates' },

    { phase: 3, name: 'GET  /api/resources/generateImage',
      method: 'GET', path: '/api/resources/generateImage' },

    { phase: 3, name: 'GET  /api/plugins',
      method: 'GET', path: '/api/plugins' },

    { phase: 3, name: 'GET  /api/scheduler (no id)',
      method: 'GET', path: '/api/scheduler' },

    { phase: 3, name: 'GET  /api/logsdir',
      method: 'GET', path: '/api/logsdir' },

    { phase: 3, name: 'GET  /api/reportsdir',
      method: 'GET', path: '/api/reportsdir' },

    { phase: 3, name: 'GET  /api/reportsQuery',
      method: 'GET', path: '/api/reportsQuery',
      query: { query: JSON.stringify({}) } },

    // ── Phase 4: Safe write operations (--write) ─────────────────────────────
    { phase: 4, name: 'POST /api/heartbeat',
      method: 'POST', path: '/api/heartbeat', body: {} },

    { phase: 4, name: 'POST /api/runSysFunction',
      method: 'POST', path: '/api/runSysFunction',
      body: { params: { name: '__test_nonexistent__', parameters: [] } } },

    { phase: 4, name: 'POST /api/signout',
      method: 'POST', path: '/api/signout' },
];

// ─── 执行单条用例 ─────────────────────────────────────────────────────────────

async function runCase(tc, nodeToken, dotnetToken) {
    const qs = tc.query
        ? '?' + new URLSearchParams(
            Object.fromEntries(Object.entries(tc.query).map(([k, v]) => [k, String(v)]))
          ).toString()
        : '';

    const nodeUrl   = `${NODE_BASE}${tc.path}${qs}`;
    const dotnetUrl = `${DOTNET_BASE}${tc.path}${qs}`;

    const [nodeRes, dotnetRes] = await Promise.all([
        fetchSafe(nodeUrl,   buildOptions(tc.method, tc.body, nodeToken)),
        fetchSafe(dotnetUrl, buildOptions(tc.method, tc.body, dotnetToken)),
    ]);

    const result = { status: 'PASS', lines: [] };

    // Connection failures
    if (nodeRes.error || dotnetRes.error) {
        result.status = 'FAIL';
        if (nodeRes.error)   result.lines.push(`  ${C.red}Node.js unreachable: ${nodeRes.error}${C.reset}`);
        if (dotnetRes.error) result.lines.push(`  ${C.red}.NET    unreachable: ${dotnetRes.error}${C.reset}`);
        return result;
    }

    // HTTP status comparison
    if (nodeRes.status !== dotnetRes.status) {
        result.status = 'DIFF';
        result.lines.push(
            `  Status   Node.js ${C.cyan}${nodeRes.status}${C.reset}  vs  .NET ${C.cyan}${dotnetRes.status}${C.reset}`
        );
    }

    // Body diff
    const diffs = deepDiff(nodeRes.body, dotnetRes.body);
    if (diffs.length > 0) {
        if (result.status !== 'FAIL') result.status = 'DIFF';
        const show = diffs.slice(0, 12);
        result.lines.push(`  Body differences (${diffs.length} total):`);
        for (const d of show) {
            result.lines.push(`  ${C.dim}${d.path}${C.reset}`);
            result.lines.push(`    Node.js : ${C.cyan}${truncate(d.nodeVal)}${C.reset}`);
            result.lines.push(`    .NET    : ${C.cyan}${truncate(d.dotnetVal)}${C.reset}`);
        }
        if (diffs.length > 12) {
            result.lines.push(`  ${C.dim}... and ${diffs.length - 12} more differences${C.reset}`);
        }
    }

    return result;
}

// ─── 主流程 ───────────────────────────────────────────────────────────────────

async function signin() {
    const body = JSON.stringify({ username: USERNAME, password: PASSWORD });
    const opts = { method: 'POST', headers: { 'Content-Type': 'application/json' }, body };

    const [nodeRes, dotnetRes] = await Promise.all([
        fetchSafe(`${NODE_BASE}/api/signin`,   opts),
        fetchSafe(`${DOTNET_BASE}/api/signin`, opts),
    ]);

    const nodeToken   = nodeRes.body?.data?.token   ?? null;
    const dotnetToken = dotnetRes.body?.data?.token ?? null;

    return { nodeRes, dotnetRes, nodeToken, dotnetToken };
}

async function main() {
    console.log(`\n${C.bold}${'='.repeat(55)}${C.reset}`);
    console.log(`${C.bold}  FUXA API Comparison: Node.js vs .NET${C.reset}`);
    console.log(`  Node.js : ${C.cyan}${NODE_BASE}${C.reset}`);
    console.log(`  .NET    : ${C.cyan}${DOTNET_BASE}${C.reset}`);
    console.log(`  User    : ${USERNAME}`);
    console.log(`  Write   : ${INCLUDE_WRITE ? C.green + 'enabled' : C.dim + 'disabled (--write to enable)'}${C.reset}`);
    console.log(`${C.bold}${'='.repeat(55)}${C.reset}\n`);

    const summary = { PASS: 0, DIFF: 0, FAIL: 0, SKIP: 0 };

    // ── Phase 1: Public ───────────────────────────────────────────────────────
    console.log(`${C.bold}Phase 1: Public endpoints${C.reset}`);
    console.log('-'.repeat(55));

    for (const tc of CASES.filter(c => c.phase === 1)) {
        const r = await runCase(tc, null, null);
        console.log(`  ${tc.name.padEnd(40)} ${badge(r.status)}`);
        r.lines.forEach(l => console.log(l));
        if (r.lines.length) console.log('');
        summary[r.status]++;
    }
    console.log('');

    // ── Phase 2: Authentication ───────────────────────────────────────────────
    console.log(`${C.bold}Phase 2: Authentication (signin)${C.reset}`);
    console.log('-'.repeat(55));

    const { nodeRes: nSignin, dotnetRes: dSignin, nodeToken, dotnetToken } = await signin();

    // Compare signin responses (exclude token field — always different)
    const signinResult = { status: 'PASS', lines: [] };
    if (nSignin.error || dSignin.error) {
        signinResult.status = 'FAIL';
        if (nSignin.error)   signinResult.lines.push(`  ${C.red}Node.js error: ${nSignin.error}${C.reset}`);
        if (dSignin.error)   signinResult.lines.push(`  ${C.red}.NET    error: ${dSignin.error}${C.reset}`);
    } else {
        if (nSignin.status !== dSignin.status) {
            signinResult.status = 'DIFF';
            signinResult.lines.push(
                `  Status   Node.js ${C.cyan}${nSignin.status}${C.reset}  vs  .NET ${C.cyan}${dSignin.status}${C.reset}`
            );
        }
        const diffs = deepDiff(nSignin.body, dSignin.body);
        // token 永远不同，排除
        const filtered = diffs.filter(d =>
            !d.path.toLowerCase().includes('token')
        );
        if (filtered.length > 0) {
            signinResult.status = 'DIFF';
            signinResult.lines.push(`  Body differences (token excluded, ${filtered.length} total):`);
            for (const d of filtered) {
                signinResult.lines.push(`  ${C.dim}${d.path}${C.reset}`);
                signinResult.lines.push(`    Node.js : ${C.cyan}${truncate(d.nodeVal)}${C.reset}`);
                signinResult.lines.push(`    .NET    : ${C.cyan}${truncate(d.dotnetVal)}${C.reset}`);
            }
        }
    }

    const nTok = nodeToken   ? `${C.green}OK${C.reset}` : `${C.red}NO TOKEN${C.reset}`;
    const dTok = dotnetToken ? `${C.green}OK${C.reset}` : `${C.red}NO TOKEN${C.reset}`;

    console.log(`  ${'POST /api/signin'.padEnd(40)} ${badge(signinResult.status)}`);
    console.log(`  Tokens:  Node.js ${nTok}  |  .NET ${dTok}`);
    signinResult.lines.forEach(l => console.log(l));
    summary[signinResult.status]++;
    console.log('');

    // ── Phase 3: Authenticated reads ──────────────────────────────────────────
    console.log(`${C.bold}Phase 3: Authenticated reads${C.reset}`);
    console.log('-'.repeat(55));

    if (!nodeToken && !dotnetToken) {
        console.log(`  ${C.yellow}Both tokens unavailable - skipping all auth tests${C.reset}`);
        const cnt = CASES.filter(c => c.phase === 3).length;
        summary.SKIP += cnt;
    } else {
        for (const tc of CASES.filter(c => c.phase === 3)) {
            const r = await runCase(tc, nodeToken, dotnetToken);
            console.log(`  ${tc.name.padEnd(40)} ${badge(r.status)}`);
            r.lines.forEach(l => console.log(l));
            if (r.lines.length) console.log('');
            summary[r.status]++;
        }
    }
    console.log('');

    // ── Phase 4: Optional write operations ───────────────────────────────────
    if (INCLUDE_WRITE) {
        console.log(`${C.bold}Phase 4: Write operations${C.reset}`);
        console.log('-'.repeat(55));

        for (const tc of CASES.filter(c => c.phase === 4)) {
            const r = await runCase(tc, nodeToken, dotnetToken);
            console.log(`  ${tc.name.padEnd(40)} ${badge(r.status)}`);
            r.lines.forEach(l => console.log(l));
            if (r.lines.length) console.log('');
            summary[r.status]++;
        }
        console.log('');
    } else {
        console.log(`${C.dim}Phase 4: Write operations - skipped (add --write to enable)${C.reset}\n`);
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    const total = summary.PASS + summary.DIFF + summary.FAIL + summary.SKIP;
    console.log(`${C.bold}${'='.repeat(55)}${C.reset}`);
    console.log(
        `${C.bold}Summary (${total} tests):${C.reset}  ` +
        `${C.green}${summary.PASS} PASS${C.reset}  ` +
        `${C.yellow}${summary.DIFF} DIFF${C.reset}  ` +
        `${C.red}${summary.FAIL} FAIL${C.reset}  ` +
        `${C.dim}${summary.SKIP} SKIP${C.reset}`
    );
    console.log(`${C.bold}${'='.repeat(55)}${C.reset}\n`);

    process.exit(summary.FAIL > 0 ? 1 : 0);
}

main().catch(err => {
    console.error(`\n${C.red}Fatal: ${err.message}${C.reset}\n`);
    process.exit(2);
});
