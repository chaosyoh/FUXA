/**
 * FUXA Database Migration - Configuration File
 * 
 * Copy this file to config.js and modify as needed.
 * 
 * Usage:
 *   cp config.example.js config.js
 *   # edit config.js
 *   node index.js --dry-run    # Preview migration
 *   node index.js --run        # Execute migration
 */

module.exports = {
  // ============================================
  // Source: FUXA _appdata directory
  // ============================================
  // Path to the FUXA server _appdata directory containing SQLite databases.
  // Typical locations:
  //   Windows:  'C:\\path\\to\\fuxa\\server\\_appdata'
  //   Linux:    '/opt/fuxa/server/_appdata'
  //   Docker:   '/usr/src/app/FUXA/server/_appdata'
  appdataDir: 'C:\\path\\to\\fuxa\\server\\_appdata',

  // ============================================
  // Target Database Configuration
  // ============================================
  target: {
    // Database type: 'mysql' or 'mssql' (SQL Server)
    type: 'mysql',

    // Connection settings
    host: 'localhost',
    port: 3306,           // MySQL: 3306, SQL Server: 1433
    user: 'root',
    password: 'your_password',
    database: 'fuxa',     // Target database name (must exist)

    // SQL Server specific options (ignored for MySQL)
    encrypt: false,
    trustServerCertificate: true,
    requestTimeout: 60000,  // ms
  },

  // ============================================
  // SQL Server Example (uncomment to use)
  // ============================================
  // target: {
  //   type: 'mssql',
  //   host: 'localhost',
  //   port: 1433,
  //   user: 'sa',
  //   password: 'your_password',
  //   database: 'fuxa',
  //   encrypt: false,
  //   trustServerCertificate: true,
  //   requestTimeout: 60000,
  // },
};
