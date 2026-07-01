/**
 * FUXA Database Migration Engine
 * Orchestrates the migration from SQLite to MySQL/SQL Server
 */

'use strict';

const path = require('path');
const SqliteReader = require('./sqlite-reader');
const TargetWriter = require('./target-writer');
const { DATABASES, DAQ_DATABASES } = require('./schema');

class Migrator {
  /**
   * @param {object} config
   * @param {string} config.appdataDir - FUXA _appdata directory path
   * @param {object} config.target - target database config
   * @param {object} [options]
   * @param {boolean} [options.dryRun] - if true, only report what would be migrated
   * @param {string[]} [options.databases] - specific databases to migrate (all if not set)
   * @param {boolean} [options.includeDaq] - include DAQ time-series data
   * @param {number} [options.batchSize] - batch size for large inserts
   * @param {boolean} [options.truncate] - truncate target tables before insert
   * @param {function} [options.onProgress] - progress callback (dbName, tableName, current, total)
   */
  constructor(config, options = {}) {
    this.config = config;
    this.options = {
      dryRun: false,
      databases: null,
      includeDaq: false,
      batchSize: 1000,
      truncate: true,
      onProgress: null,
      ...options,
    };
    this.reader = new SqliteReader(config.appdataDir);
    this.writer = new TargetWriter(config.target);
    this.stats = {
      databases: 0,
      tables: 0,
      rows: 0,
      skipped: 0,
      errors: [],
      migratedTables: [],  // [{ database, table, rows }]
    };
  }

  /**
   * Run the migration
   * @returns {object} migration statistics
   */
  async run() {
    this._log('Connecting to target database...');

    if (!this.options.dryRun) {
      await this.writer.connect();
      this._log('Connected to target database.');

      // Test connection
      await this.writer.testConnection();
      this._log('Target database connection verified.');
    } else {
      this._log('[DRY RUN] No data will be written.');
    }

    // Scan and migrate standard databases
    for (const dbDef of DATABASES) {
      if (this.options.databases && !this.options.databases.includes(dbDef.name)) {
        continue;
      }

      const dbFile = dbDef.dbFile;
      if (!this.reader.exists(dbFile)) {
        this._log(`  Skipping '${dbDef.name}' - file not found: ${dbFile}`);
        this.stats.skipped++;
        continue;
      }

      await this._migrateDatabase(dbDef, dbFile);
    }

    // Migrate DAQ databases if requested
    if (this.options.includeDaq) {
      await this._migrateDaqDatabases();
    }

    // Close connections
    this.reader.closeAll();
    if (!this.options.dryRun) {
      await this.writer.close();
    }

    return this.stats;
  }

  /**
   * Migrate a single database
   * @param {object} dbDef - database definition from schema
   * @param {string} dbFile - SQLite file name
   */
  async _migrateDatabase(dbDef, dbFile) {
    this._log(`\nMigrating database: ${dbDef.name} (${dbFile})`);
    this.stats.databases++;

    const existingTables = this.reader.getTableNames(dbFile);

    for (const tableDef of dbDef.tables) {
      // Check if table exists in SQLite
      if (!existingTables.includes(tableDef.name)) {
        this._log(`  Table '${tableDef.name}' not found, skipping.`);
        this.stats.skipped++;
        continue;
      }

      const rowCount = this.reader.count(dbFile, tableDef.name);
      const targetTable = tableDef.targetTable || tableDef.name;

      if (this.options.dryRun) {
        this.stats.tables++;
        this.stats.rows += rowCount;
        this.stats.migratedTables.push({ database: dbDef.name, table: `${tableDef.name} -> ${targetTable}`, rows: rowCount });
        continue;
      }

      try {
        // Create target table (drop first if truncate is enabled to ensure correct schema)
        await this.writer.createTable(targetTable, tableDef, null, this.options.truncate);

        let inserted = 0;
        if (rowCount > 0) {
          // Read all data from SQLite
          const rows = this.reader.readAll(dbFile, tableDef.name);

          // Insert into target
          inserted = await this.writer.insertRows(
            targetTable,
            rows,
            tableDef,
            null,
            this.options.batchSize
          );
          this.stats.rows += inserted;
        }

        const mapping = tableDef.targetTable ? ` (${tableDef.name} -> ${targetTable})` : '';
        this._log(`    ${dbDef.name}.${tableDef.name}${mapping} -> ${inserted} rows`);
        this.stats.tables++;
        this.stats.migratedTables.push({ database: dbDef.name, table: targetTable, rows: inserted });

        if (this.options.onProgress) {
          this.options.onProgress(dbDef.name, targetTable, inserted, rowCount);
        }
      } catch (err) {
        const errMsg = `Error migrating ${dbDef.name}.${tableDef.name}: ${err.message}`;
        this._log(`    ERROR: ${errMsg}`);
        this.stats.errors.push(errMsg);
      }
    }
  }

  /**
   * Migrate DAQ (time-series) databases
   */
  async _migrateDaqDatabases() {
    this._log('\nMigrating DAQ databases...');

    for (const daqDef of DAQ_DATABASES) {
      const daqFiles = this.reader.findDaqFiles(daqDef.dbPrefix);

      if (daqFiles.length === 0) {
        this._log(`  No DAQ files found for prefix '${daqDef.dbPrefix}'.`);
        continue;
      }

      this._log(`  Found ${daqFiles.length} files for '${daqDef.name}'.`);

      for (const daqFile of daqFiles) {
        // Generate a target table prefix based on the file name
        const baseName = path.basename(daqFile, '.db');
        const tablePrefix = baseName.replace(/[^a-zA-Z0-9_]/g, '_');

        this._log(`  Processing: ${daqFile}`);

        for (const tableDef of daqDef.tables) {
          const rowCount = this.reader.count(daqFile, tableDef.name);

          if (this.options.dryRun) {
            this.stats.tables++;
            this.stats.rows += rowCount;
            this.stats.migratedTables.push({ database: daqDef.name, table: `${tablePrefix}_${tableDef.name}`, rows: rowCount });
            continue;
          }

          try {
            // Create target table with prefix (drop first if truncate is enabled)
            await this.writer.createTable(tableDef.name, tableDef, tablePrefix, this.options.truncate);

            let totalInserted = 0;
            if (rowCount > 0) {
              // Read and insert in batches for DAQ data (can be large)
              this.reader.readAllBatched(daqFile, tableDef.name, this.options.batchSize, async (batch, offset, total) => {
                const inserted = await this.writer.insertRows(
                  tableDef.name,
                  batch,
                  tableDef,
                  tablePrefix,
                  this.options.batchSize
                );
                totalInserted += inserted;
                this._log(`    Batch: ${offset + batch.length}/${total} rows`);
              });
              this.stats.rows += totalInserted;
            }

            this._log(`    ${daqDef.name}.${tablePrefix}_${tableDef.name} -> ${totalInserted} rows`);
            this.stats.tables++;
            this.stats.migratedTables.push({ database: daqDef.name, table: `${tablePrefix}_${tableDef.name}`, rows: totalInserted });
          } catch (err) {
            const errMsg = `Error migrating DAQ ${daqFile}.${tableDef.name}: ${err.message}`;
            this._log(`    ERROR: ${errMsg}`);
            this.stats.errors.push(errMsg);
          }
        }
      }
    }
  }

  /**
   * Get a summary of all databases and their row counts (for preview)
   * @returns {Array} summary info
   */
  getSummary() {
    const summary = [];

    for (const dbDef of DATABASES) {
      if (!this.reader.exists(dbDef.dbFile)) {
        continue;
      }
      const existingTables = this.reader.getTableNames(dbDef.dbFile);
      const tables = [];
      for (const tableDef of dbDef.tables) {
        if (existingTables.includes(tableDef.name)) {
          const count = this.reader.count(dbDef.dbFile, tableDef.name);
          tables.push({ name: tableDef.name, rows: count });
        }
      }
      summary.push({
        name: dbDef.name,
        dbFile: dbDef.dbFile,
        tables,
      });
    }

    // DAQ databases
    if (this.options.includeDaq) {
      for (const daqDef of DAQ_DATABASES) {
        const daqFiles = this.reader.findDaqFiles(daqDef.dbPrefix);
        if (daqFiles.length > 0) {
          const tables = [];
          for (const file of daqFiles) {
            for (const tableDef of daqDef.tables) {
              const count = this.reader.count(file, tableDef.name);
              tables.push({ name: `${file}:${tableDef.name}`, rows: count });
            }
          }
          summary.push({
            name: daqDef.name,
            dbFile: `${daqDef.dbPrefix}*.db (${daqFiles.length} files)`,
            tables,
          });
        }
      }
    }

    return summary;
  }

  _log(msg) {
    console.log(msg);
  }
}

module.exports = Migrator;
