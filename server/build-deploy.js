/**
 * FUXA Server 部署打包脚本
 * 
 * 1. 使用 esbuild 打包纯 JS 依赖
 * 2. 复制 external 原生/动态模块到 dist/node_modules
 * 3. 生成最小化的部署目录
 * 
 * 使用方式: node build-deploy.js
 * 输出目录: dist/
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Node.js 内置模块（不需要从 node_modules 复制）
const BUILTINS = new Set([
  'assert', 'async_hooks', 'buffer', 'child_process', 'cluster',
  'console', 'constants', 'crypto', 'dgram', 'dns', 'events',
  'fs', 'http', 'http2', 'https', 'inspector', 'module', 'net',
  'os', 'path', 'perf_hooks', 'process', 'punycode', 'querystring',
  'readline', 'repl', 'stream', 'string_decoder', 'timers', 'tls',
  'trace_events', 'tty', 'url', 'util', 'v8', 'vm', 'worker_threads',
  'zlib', '<runtime>'
]);

// 可选模块（未安装时不报错）
const OPTIONAL_MODULES = new Set([
  'chartjs-node-canvas', 'node-webcam', 'onoff', 'pg-native',
  'redis', 'mcprotocol', 'nodepccc'
]);

const DIST_DIR = path.join(__dirname, 'dist');
const DIST_MODULES = path.join(DIST_DIR, 'node_modules');
const SRC_MODULES = path.join(__dirname, 'node_modules');

function copyDirSync(src, dest) {
  if (!fs.existsSync(src)) return false;
  fs.mkdirSync(dest, { recursive: true });
  const entries = fs.readdirSync(src, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      copyDirSync(srcPath, destPath);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
  return true;
}

function getDirSize(dir) {
  let size = 0;
  if (!fs.existsSync(dir)) return 0;
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      size += getDirSize(fullPath);
    } else {
      size += fs.statSync(fullPath).size;
    }
  }
  return size;
}

// 递归查找一个包的所有依赖
function findPackageDeps(pkgName, visited = new Set()) {
  if (visited.has(pkgName) || BUILTINS.has(pkgName)) return;
  visited.add(pkgName);

  // 处理 scoped 包
  const pkgDir = path.join(SRC_MODULES, ...pkgName.split('/'));
  const pkgJson = path.join(pkgDir, 'package.json');
  
  if (!fs.existsSync(pkgJson)) return;
  
  try {
    const pkg = JSON.parse(fs.readFileSync(pkgJson, 'utf-8'));
    const deps = Object.keys(pkg.dependencies || {});
    for (const dep of deps) {
      findPackageDeps(dep, visited);
    }
  } catch (e) {}
}

// 清理 node_modules 中运行时不需要的文件
function cleanNodeModules(modulesDir) {
  let freedBytes = 0;

  // 需要完全删除的目录名
  const REMOVE_DIRS = new Set([
    'test', 'tests', '__tests__', 'spec', '__mocks__',
    'example', 'examples', 'doc', 'docs', 'documentation',
    '.github', '.vscode', '.idea', 'coverage', '.nyc_output',
    'benchmark', 'benchmarks', 'man', 'website',
  ]);

  // 可以安全删除 src 的条件：存在 dist 或 lib 目录
  // 注意：很多包的入口在 src/ 下（如 long），不能轻易删除
  // 因此禁用此优化以确保安全
  const SRC_SAFE_IF_HAS = []; // 禁用 src 删除

  // 需要删除的文件模式（不删除 .ts/.tsx 源文件，因为有些包运行时需要）
  const REMOVE_FILE_PATTERNS = [
    /\.d\.ts$/,
    /\.d\.ts\.map$/,
    /\.js\.map$/,
    /\.coffee$/,
    /\.md$/i,
    /\.markdown$/i,
    /^CHANGELOG(\.|$)/i,
    /^HISTORY(\.|$)/i,
    /^CHANGES(\.|$)/i,
    /\.eslintrc/,
    /\.prettierrc/,
    /^tsconfig/,
    /\.travis\.yml$/,
    /\.editorconfig$/,
    /\.jshintrc$/,
    /\.npmignore$/,
    /^Makefile$/,
    /^Gruntfile/i,
    /^Gulpfile/i,
    /\.coveralls\.yml$/,
    /^appveyor\.yml$/,
    /\.babelrc/,
    /^\.nyc/,
    /^jest\.config/,
    /^karma\.conf/,
    /^\.eslint/,
    /^\.prettier/,
    /^bower\.json$/,
    /^component\.json$/,
    /^\.DS_Store$/,
    /^Thumbs\.db$/,
  ];

  // 需要完全删除的顶层包
  const REMOVE_TOP_LEVEL = ['@types'];

  // 删除 @types（运行时不需要 TypeScript 类型定义）
  for (const topDir of REMOVE_TOP_LEVEL) {
    const fullPath = path.join(modulesDir, topDir);
    if (fs.existsSync(fullPath)) {
      const size = getDirSize(fullPath);
      fs.rmSync(fullPath, { recursive: true });
      freedBytes += size;
    }
  }

  function cleanDir(dir, depth) {
    if (!fs.existsSync(dir)) return;
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch (e) { return; }

    const entryNames = entries.map(e => e.name);

    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);

      if (entry.isDirectory()) {
        if (depth >= 2 && REMOVE_DIRS.has(entry.name)) {
          const size = getDirSize(fullPath);
          fs.rmSync(fullPath, { recursive: true });
          freedBytes += size;
          continue;
        }
        // 对于 src 目录，只在有编译输出目录时才删除
        if (depth >= 2 && entry.name === 'src') {
          if (SRC_SAFE_IF_HAS.some(d => entryNames.includes(d))) {
            const size = getDirSize(fullPath);
            fs.rmSync(fullPath, { recursive: true });
            freedBytes += size;
            continue;
          }
        }
        cleanDir(fullPath, depth + 1);
      } else if (entry.isFile() && depth >= 2) {
        if (entry.name === 'package.json') continue;
        if (entry.name.startsWith('LICENSE')) continue;
        if (entry.name.startsWith('license')) continue;

        const shouldRemove = REMOVE_FILE_PATTERNS.some(p => p.test(entry.name));
        if (shouldRemove) {
          const size = fs.statSync(fullPath).size;
          fs.rmSync(fullPath);
          freedBytes += size;
        }
      }
    }
  }

  cleanDir(modulesDir, 0);
  return freedBytes;
}

async function main() {
  console.log('=== FUXA Server 部署打包 ===\n');

  // Step 0: 编译 TypeScript（部分源码需要先编译）
  console.log('[0/5] 编译 TypeScript...');
  execSync('npx tsc', { stdio: 'inherit', cwd: __dirname });
  console.log('  TypeScript 编译完成');

  // Step 1: 运行 esbuild 打包
  console.log('\n[1/5] 运行 esbuild 打包...');
  execSync('node esbuild.config.js', { stdio: 'inherit', cwd: __dirname });

  // Step 2: 读取 external 模块列表
  console.log('\n[2/4] 分析 external 模块依赖...');
  const externals = JSON.parse(fs.readFileSync(path.join(DIST_DIR, 'externals.json'), 'utf-8'));
  
  // 过滤掉内置模块，得到需要复制的 npm 包
  const npmPackages = externals.filter(m => !BUILTINS.has(m));
  console.log(`  需要复制的 npm 包: ${npmPackages.length} 个`);

  // Step 3: 复制 external 模块及其依赖
  console.log('\n[3/4] 复制 external 模块到 dist/node_modules...');
  
  // 清空旧的 dist/node_modules
  if (fs.existsSync(DIST_MODULES)) {
    fs.rmSync(DIST_MODULES, { recursive: true });
  }
  fs.mkdirSync(DIST_MODULES, { recursive: true });

  // 收集所有需要的包（包括 external 的依赖树）
  const allNeeded = new Set();
  for (const pkg of npmPackages) {
    findPackageDeps(pkg, allNeeded);
  }
  // 去掉 builtins
  for (const b of BUILTINS) allNeeded.delete(b);

  let copied = 0;
  let skipped = 0;
  for (const pkg of [...allNeeded].sort()) {
    const srcDir = path.join(SRC_MODULES, ...pkg.split('/'));
    const destDir = path.join(DIST_MODULES, ...pkg.split('/'));
    
    if (fs.existsSync(srcDir)) {
      copyDirSync(srcDir, destDir);
      copied++;
    } else if (OPTIONAL_MODULES.has(pkg)) {
      skipped++;
    } else {
      console.log(`  ⚠ 未找到: ${pkg} (可能是可选依赖)`);
      skipped++;
    }
  }
  console.log(`  已复制: ${copied} 个包, 跳过: ${skipped} 个可选包`);

  // Step 3.5: 清理不必要的文件（大幅缩减体积）
  console.log('\n[3.5/5] 清理 node_modules 中不必要的文件...');
  let cleanedSize = 0;
  cleanedSize += cleanNodeModules(DIST_MODULES);
  console.log(`  清理释放: ${(cleanedSize / 1024 / 1024).toFixed(2)} MB`);

  // Step 4: 复制其他必要文件
  console.log('\n[4/5] 复制其他必要文件...');
  
  // 复制静态资源
  const filesToCopy = ['settings.default.js', 'paths.js', 'envParams.js'];
  for (const f of filesToCopy) {
    const src = path.join(__dirname, f);
    if (fs.existsSync(src)) {
      fs.copyFileSync(src, path.join(DIST_DIR, f));
      console.log(`  复制: ${f}`);
    }
  }

  // 复制目录
  const dirsToCopy = ['views', 'config', 'docs'];
  for (const d of dirsToCopy) {
    const src = path.join(__dirname, d);
    if (fs.existsSync(src)) {
      copyDirSync(src, path.join(DIST_DIR, d));
      console.log(`  复制目录: ${d}/`);
    }
  }

  // 复制前端编译产物到 dist/dist/（server 代码中的 fallback 路径）
  const clientDistSrc = path.resolve(__dirname, '../client/dist');
  const clientDistDest = path.join(DIST_DIR, 'dist');
  if (fs.existsSync(clientDistSrc)) {
    copyDirSync(clientDistSrc, clientDistDest);
    console.log(`  复制前端: client/dist/ → dist/dist/`);
  } else {
    console.log(`  ⚠ 未找到 client/dist/，跳过前端文件复制（运行时需要手动指定 httpStatic）`);
  }

  // 创建最小化的 package.json
  const distPkg = {
    name: 'fuxa-server',
    version: require('./package.json').version,
    main: 'server.js',
    scripts: {
      start: 'node server.js'
    }
  };
  fs.writeFileSync(path.join(DIST_DIR, 'package.json'), JSON.stringify(distPkg, null, 2));
  console.log('  生成: package.json');

  // 统计大小
  console.log('\n=== 打包结果 ===');
  const bundleSize = fs.statSync(path.join(DIST_DIR, 'server.js')).size;
  const modulesSize = getDirSize(DIST_MODULES);
  const totalSize = getDirSize(DIST_DIR);
  const originalSize = getDirSize(SRC_MODULES);

  console.log(`  Bundle (server.js):     ${(bundleSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  External node_modules:  ${(modulesSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  dist/ 总大小:           ${(totalSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  原始 node_modules:      ${(originalSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  体积缩减:              ${((1 - totalSize / originalSize) * 100).toFixed(1)}%`);
  console.log(`\n部署目录: ${DIST_DIR}`);
  console.log('启动命令: cd dist && node server.js');
}

main().catch(err => {
  console.error('打包失败:', err);
  process.exit(1);
});
