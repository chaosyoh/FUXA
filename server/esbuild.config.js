/**
 * FUXA Server esbuild 打包配置
 * 
 * 将纯JS依赖打包进 bundle，原生模块和动态加载模块保留为 external。
 * 打包后只需保留少量 external 模块的 node_modules，体积大幅缩小。
 * 
 * 使用方式: node esbuild.config.js
 */

const esbuild = require('esbuild');
const fs = require('fs');
const path = require('path');

// 必须保留为 external 的模块（原生模块 + 动态加载 + 复杂运行时依赖）
const EXTERNAL_MODULES = [
  // 原生 C++ addon 模块
  'sqlite3',
  'odbc',
  'node-snap7',
  'modbus-serial',
  'serialport',
  '@serialport/*',

  // 非常大且有动态 require 的模块
  'node-opcua',
  'node-opcua-*',

  // 动态插件加载
  'node-red',
  'node-red-*',
  '@node-red/*',
  'live-plugin-manager',

  // 加载二进制/数据文件的模块（运行时依赖 __dirname 读取文件）
  'pdfmake',
  '@foliojs-fork/*',
  'fontkit',

  // Node.js 内置模块
  'fs', 'path', 'http', 'https', 'net', 'tls', 'os', 'crypto',
  'stream', 'events', 'util', 'url', 'querystring', 'zlib',
  'child_process', 'cluster', 'dgram', 'dns', 'readline',
  'buffer', 'string_decoder', 'assert', 'constants', 'worker_threads',
  'perf_hooks', 'inspector', 'v8', 'vm', 'tty',
];

async function build() {
  const startTime = Date.now();

  try {
    const result = await esbuild.build({
      entryPoints: ['main.js'],
      bundle: true,
      platform: 'node',
      target: 'node18',
      outfile: 'dist/server.js',
      format: 'cjs',
      sourcemap: true,
      minify: false, // 保持可读性，便于调试
      keepNames: true, // 保留函数名，方便错误追踪
      external: EXTERNAL_MODULES,
      // 处理 __dirname/__filename（bundled 后路径会变）
      define: {
        // 不重定义，让 esbuild 使用默认行为
      },
      // 允许动态 require 不被打包（如 settings 文件）
      logLevel: 'warning',
      metafile: true,
    });

    // 输出打包分析
    const meta = result.metafile;
    const outputFile = Object.keys(meta.outputs)[0];
    const outputSize = meta.outputs[outputFile].bytes;
    
    console.log(`\n✓ 打包完成！`);
    console.log(`  输出文件: dist/server.js`);
    console.log(`  Bundle 大小: ${(outputSize / 1024 / 1024).toFixed(2)} MB`);
    console.log(`  耗时: ${Date.now() - startTime}ms`);

    // 生成依赖分析报告
    const text = await esbuild.analyzeMetafile(meta, { verbose: false });
    fs.writeFileSync('dist/bundle-analysis.txt', text);
    console.log(`  分析报告: dist/bundle-analysis.txt`);

    // 生成需要保留的 external 模块列表
    const externalImports = new Set();
    for (const [file, info] of Object.entries(meta.inputs)) {
      if (info.imports) {
        for (const imp of info.imports) {
          if (imp.external) {
            // 提取顶层包名
            const parts = imp.path.split('/');
            const pkgName = parts[0].startsWith('@') ? parts[0] + '/' + parts[1] : parts[0];
            externalImports.add(pkgName);
          }
        }
      }
    }

    console.log(`\n  需要保留的 external 模块 (${externalImports.size} 个):`);
    const sorted = [...externalImports].sort();
    sorted.forEach(m => console.log(`    - ${m}`));

    // 写入列表文件，供后续脚本使用
    fs.writeFileSync('dist/externals.json', JSON.stringify(sorted, null, 2));
    console.log(`\n  外部模块列表已保存到: dist/externals.json`);

  } catch (error) {
    console.error('打包失败:', error);
    process.exit(1);
  }
}

build();
