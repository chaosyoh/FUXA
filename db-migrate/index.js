/**
 * FUXA Database Migration Tool
 * 
 * Migrates FUXA SQLite databases to MySQL or SQL Server.
 * 
 * Usage:
 *   node index.js                  # Interactive mode - preview & confirm
 *   node index.js --dry-run        # Preview only, no data written
 *   node index.js --run            # Execute migration directly
 *   node index.js --config path    # Use custom config file
 */

'use strict';

require('dotenv').config();
const { Command } = require('commander');
const chalk = require('chalk');
const ora = require('ora');
const path = require('path');
const fs = require('fs');
const Migrator = require('./src/migrate');

const program = new Command();

program
  .name('fuxa-db-migrate')
  .description('Migrate FUXA SQLite databases to MySQL or SQL Server')
  .version('1.0.0')
  .option('--run', 'Execute migration (skip confirmation)')
  .option('--dry-run', 'Preview migration without writing data')
  .option('--config <path>', 'Path to config file', './config.js')
  .option('--include-daq', 'Include DAQ time-series data migration')
  .option('--batch-size <number>', 'Batch size for large inserts', '1000')
  .option('--no-truncate', 'Do not truncate target tables before insert')
  .option('--db <names...>', 'Specific databases to migrate (comma-separated)');

program.parse(process.argv);
const opts = program.opts();

// Load configuration
let config;
const configPath = path.resolve(opts.config);
if (fs.existsSync(configPath)) {
  config = require(configPath);
} else {
  // Try to build config from environment variables
  config = buildConfigFromEnv();
  if (!config) {
    console.error(chalk.red(`Config file not found: ${configPath}`));
    console.error(chalk.yellow('Create a config.js file or set environment variables.'));
    console.error(chalk.yellow('See config.example.js for reference.'));
    process.exit(1);
  }
}

// Validate config
if (!config.appdataDir) {
  console.error(chalk.red('Error: appdataDir is required in config.'));
  process.exit(1);
}
if (!fs.existsSync(config.appdataDir)) {
  console.error(chalk.red(`Error: appdataDir does not exist: ${config.appdataDir}`));
  process.exit(1);
}
if (!config.target || !config.target.type) {
  console.error(chalk.red('Error: target database configuration is required.'));
  process.exit(1);
}

async function main() {
  console.log(chalk.bold.cyan('\n=== FUXA Database Migration Tool ===\n'));

  const migrateOptions = {
    dryRun: opts.dryRun || false,
    includeDaq: opts.includeDaq || false,
    batchSize: parseInt(opts.batchSize) || 1000,
    truncate: opts.truncate !== false,
    databases: opts.db ? opts.db : null,
  };

  const migrator = new Migrator(config, migrateOptions);

  // Step 1: Show summary
  console.log(chalk.bold('Scanning FUXA databases...\n'));
  const summary = migrator.getSummary();

  if (summary.length === 0) {
    console.log(chalk.yellow('No FUXA databases found in: ' + config.appdataDir));
    process.exit(0);
  }

  let totalRows = 0;
  for (const db of summary) {
    console.log(chalk.bold(`  ${chalk.blue(db.name)} (${chalk.gray(db.dbFile)})`));
    for (const table of db.tables) {
      const rowStr = table.rows.toString().padStart(8, ' ');
      console.log(`    ${table.name}: ${chalk.green(rowStr)} rows`);
      totalRows += table.rows;
    }
  }
  console.log(chalk.bold(`\n  Total: ${chalk.green(totalRows.toString())} rows to migrate`));
  console.log(chalk.bold(`  Target: ${chalk.magenta(config.target.type.toUpperCase())} -> ${config.target.host}:${config.target.port}/${config.target.database}`));

  if (opts.dryRun) {
    console.log(chalk.yellow('\n[DRY RUN] No data was written. Use --run to execute migration.\n'));
    migrator.reader.closeAll();
    return;
  }

  // Step 2: Confirm unless --run
  if (!opts.run) {
    console.log(chalk.yellow('\nUse --run flag to execute migration, or --dry-run to preview.\n'));
    migrator.reader.closeAll();
    return;
  }

  // Step 3: Execute migration
  const spinner = ora('Starting migration...').start();

  const result = await migrator.run();

  spinner.stop();

  // Print results
  console.log(chalk.bold.cyan('\n=== Migration Complete ===\n'));
  console.log(`  Databases processed: ${chalk.green(result.databases)}`);
  console.log(`  Tables migrated:     ${chalk.green(result.tables)}`);
  console.log(`  Rows inserted:       ${chalk.green(result.rows)}`);
  console.log(`  Skipped:             ${chalk.yellow(result.skipped)}`);

  // Print detailed table list
  if (result.migratedTables && result.migratedTables.length > 0) {
    console.log(chalk.bold('\n  Migrated tables:'));
    let lastDb = '';
    for (const item of result.migratedTables) {
      if (item.database !== lastDb) {
        console.log(chalk.bold(`\n    [${chalk.blue(item.database)}]`));
        lastDb = item.database;
      }
      const rowStr = item.rows.toString().padStart(8, ' ');
      const rowDisplay = item.rows > 0 ? chalk.green(rowStr) : chalk.gray(rowStr);
      console.log(`      ${item.table}: ${rowDisplay} rows`);
    }
  }

  if (result.errors.length > 0) {
    console.log(chalk.red(`\n  Errors (${result.errors.length}):`));
    for (const err of result.errors) {
      console.log(chalk.red(`    - ${err}`));
    }
    process.exit(1);
  } else {
    console.log(chalk.green('\n  Migration completed successfully!\n'));
  }
}

/**
 * Build config from environment variables
 */
function buildConfigFromEnv() {
  const appdataDir = process.env.FUXA_APPDATA_DIR;
  const targetType = process.env.TARGET_TYPE; // 'mysql' or 'mssql'

  if (!appdataDir || !targetType) {
    return null;
  }

  return {
    appdataDir,
    target: {
      type: targetType,
      host: process.env.TARGET_HOST || 'localhost',
      port: parseInt(process.env.TARGET_PORT) || (targetType === 'mysql' ? 3306 : 1433),
      user: process.env.TARGET_USER || 'root',
      password: process.env.TARGET_PASSWORD || '',
      database: process.env.TARGET_DATABASE || 'fuxa',
      encrypt: process.env.TARGET_ENCRYPT === 'true',
      trustServerCertificate: process.env.TARGET_TRUST_CERT !== 'false',
    },
  };
}

main().catch(err => {
  console.error(chalk.red('\nMigration failed: ' + err.message));
  if (err.stack) {
    console.error(chalk.gray(err.stack));
  }
  process.exit(1);
});
