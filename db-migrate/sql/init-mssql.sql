-- ============================================================
-- FUXA Project Initialization Script for SQL Server
-- 
-- This script creates all FUXA database tables and inserts
-- the minimum initial data required for a new project.
--
-- Usage:
--   sqlcmd -S localhost -U sa -d fuxa -i init-mssql.sql
--   or execute in SQL Server Management Studio
--
-- Note: All tables are created in a single database.
--       In the original FUXA, data is split across multiple
--       SQLite files. Here they are consolidated into one SQL Server database.
-- ============================================================

SET NOCOUNT ON;
GO

-- ============================================================
-- 1. PROJECT TABLES (from project.fuxap.db)
-- ============================================================

-- general: project-level key-value settings (version, layout, etc.)
IF OBJECT_ID('dbo.general', 'U') IS NOT NULL DROP TABLE dbo.general;
CREATE TABLE dbo.general (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_general PRIMARY KEY ([name])
);
GO

-- views: HMI view definitions (SVG canvas, gauges, etc.)
IF OBJECT_ID('dbo.views', 'U') IS NOT NULL DROP TABLE dbo.views;
CREATE TABLE dbo.views (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_views PRIMARY KEY ([name])
);
GO

-- devices: device configurations (server, PLCs, sensors, etc.)
IF OBJECT_ID('dbo.devices', 'U') IS NOT NULL DROP TABLE dbo.devices;
CREATE TABLE dbo.devices (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  [connection] NVARCHAR(MAX),
  [cntid] NVARCHAR(255),
  [cntpwd] NVARCHAR(255),
  CONSTRAINT PK_devices PRIMARY KEY ([name])
);
GO

-- devicesSecurity: device security/certificate settings
IF OBJECT_ID('dbo.devicesSecurity', 'U') IS NOT NULL DROP TABLE dbo.devicesSecurity;
CREATE TABLE dbo.devicesSecurity (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_devicesSecurity PRIMARY KEY ([name])
);
GO

-- texts: multilingual text definitions
IF OBJECT_ID('dbo.texts', 'U') IS NOT NULL DROP TABLE dbo.texts;
CREATE TABLE dbo.texts (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_texts PRIMARY KEY ([name])
);
GO

-- alarms: alarm configuration definitions
IF OBJECT_ID('dbo.alarms', 'U') IS NOT NULL DROP TABLE dbo.alarms;
CREATE TABLE dbo.alarms (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_alarms PRIMARY KEY ([name])
);
GO

-- notifications: notification configuration definitions
IF OBJECT_ID('dbo.notifications', 'U') IS NOT NULL DROP TABLE dbo.notifications;
CREATE TABLE dbo.notifications (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_notifications PRIMARY KEY ([name])
);
GO

-- scripts: user-defined scripts
IF OBJECT_ID('dbo.scripts', 'U') IS NOT NULL DROP TABLE dbo.scripts;
CREATE TABLE dbo.scripts (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_scripts PRIMARY KEY ([name])
);
GO

-- reports: report configurations
IF OBJECT_ID('dbo.reports', 'U') IS NOT NULL DROP TABLE dbo.reports;
CREATE TABLE dbo.reports (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_reports PRIMARY KEY ([name])
);
GO

-- locations: maps locations
IF OBJECT_ID('dbo.locations', 'U') IS NOT NULL DROP TABLE dbo.locations;
CREATE TABLE dbo.locations (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_locations PRIMARY KEY ([name])
);
GO

-- ============================================================
-- 2. USER TABLES (from users.fuxap.db)
-- ============================================================

-- users: user accounts
IF OBJECT_ID('dbo.users', 'U') IS NOT NULL DROP TABLE dbo.users;
CREATE TABLE dbo.users (
  [username] NVARCHAR(255) NOT NULL,
  [fullname] NVARCHAR(255),
  [password] NVARCHAR(255),
  [groups] INT,
  [info] NVARCHAR(MAX),
  CONSTRAINT PK_users PRIMARY KEY ([username])
);
GO

-- roles: user role definitions
IF OBJECT_ID('dbo.roles', 'U') IS NOT NULL DROP TABLE dbo.roles;
CREATE TABLE dbo.roles (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_roles PRIMARY KEY ([name])
);
GO

-- ============================================================
-- 3. API KEYS TABLE (from apikeys.fuxap.db)
-- ============================================================

IF OBJECT_ID('dbo.apikeys', 'U') IS NOT NULL DROP TABLE dbo.apikeys;
CREATE TABLE dbo.apikeys (
  [name] NVARCHAR(255) NOT NULL,
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_apikeys PRIMARY KEY ([name])
);
GO

-- ============================================================
-- 4. ALARM HISTORY TABLES (from alarms.fuxap.db)
-- ============================================================

-- alarms_runtime: current alarm states (separate from alarm config)
IF OBJECT_ID('dbo.alarms_runtime', 'U') IS NOT NULL DROP TABLE dbo.alarms_runtime;
CREATE TABLE dbo.alarms_runtime (
  [nametype] NVARCHAR(255) NOT NULL,
  [type] NVARCHAR(100),
  [status] NVARCHAR(100),
  [ontime] BIGINT,
  [offtime] BIGINT,
  [acktime] BIGINT,
  CONSTRAINT PK_alarms_runtime PRIMARY KEY ([nametype])
);
GO

-- alarms_chronicle: alarm history log
IF OBJECT_ID('dbo.alarms_chronicle', 'U') IS NOT NULL DROP TABLE dbo.alarms_chronicle;
CREATE TABLE dbo.alarms_chronicle (
  [Sn] INT NOT NULL IDENTITY(1,1),
  [nametype] NVARCHAR(255),
  [type] NVARCHAR(100),
  [status] NVARCHAR(100),
  [text] NVARCHAR(MAX),
  [grp] NVARCHAR(255),
  [ontime] BIGINT,
  [offtime] BIGINT,
  [acktime] BIGINT,
  [userack] NVARCHAR(255),
  CONSTRAINT PK_alarms_chronicle PRIMARY KEY ([Sn])
);
GO

-- ============================================================
-- 5. NOTIFICATION HISTORY TABLES (from notifications.fuxap.db)
-- ============================================================

IF OBJECT_ID('dbo.notifications_chronicle', 'U') IS NOT NULL DROP TABLE dbo.notifications_chronicle;
CREATE TABLE dbo.notifications_chronicle (
  [Sn] INT NOT NULL IDENTITY(1,1),
  [id] NVARCHAR(255),
  [name] NVARCHAR(255),
  [type] NVARCHAR(100),
  [receiver] NVARCHAR(255),
  [text] NVARCHAR(MAX),
  [notifytime] BIGINT,
  [notifytype] NVARCHAR(100),
  CONSTRAINT PK_notifications_chronicle PRIMARY KEY ([Sn])
);
GO

-- ============================================================
-- 6. SCHEDULER TABLE (from scheduler.db)
-- ============================================================

IF OBJECT_ID('dbo.schedulers', 'U') IS NOT NULL DROP TABLE dbo.schedulers;
CREATE TABLE dbo.schedulers (
  [id] NVARCHAR(255) NOT NULL,
  [data] NVARCHAR(MAX) NOT NULL,
  [created_at] DATETIME2 DEFAULT GETDATE(),
  [updated_at] DATETIME2 DEFAULT GETDATE(),
  CONSTRAINT PK_schedulers PRIMARY KEY ([id])
);
GO

-- ============================================================
-- 7. CURRENT TAG READINGS TABLE (from currentTagReadings.db)
-- ============================================================

IF OBJECT_ID('dbo.currentValues', 'U') IS NOT NULL DROP TABLE dbo.currentValues;
CREATE TABLE dbo.currentValues (
  [tagId] NVARCHAR(255) NOT NULL,
  [deviceId] NVARCHAR(255),
  [value] NVARCHAR(MAX),
  CONSTRAINT PK_currentValues PRIMARY KEY ([tagId])
);
GO

-- ============================================================
-- INITIALIZATION DATA
-- ============================================================

-- [general] Project version
INSERT INTO dbo.general ([name], [value]) VALUES (N'version', N'"1.01"');

-- [general] Layout settings (navigation, header, etc.)
INSERT INTO dbo.general ([name], [value]) VALUES (N'layout', N'{"autoresize":false,"start":"","navigation":{"mode":"item.navsmode-over","type":"item.navtype-icons-text-block","bkcolor":"#F4F5F7","fgcolor":"#1D1D1D"},"header":{"bkcolor":"#ffffff","fgcolor":"#000000","fontSize":13,"itemsAnchor":"left"},"showdev":true,"inputdialog":"false","hidenavigation":false,"theme":"","loginonstart":false,"loginoverlaycolor":"none","show_connection_error":true,"customStyles":""}');

-- [general] Mobile layout settings
INSERT INTO dbo.general ([name], [value]) VALUES (N'mobileLayout', N'{"autoresize":false,"start":"","navigation":{"mode":"item.navsmode-over","type":"item.navtype-icons-text-block","bkcolor":"#F4F5F7","fgcolor":"#1D1D1D"},"header":{"bkcolor":"#ffffff","fgcolor":"#000000","fontSize":13,"itemsAnchor":"left"},"showdev":true,"inputdialog":"false","hidenavigation":false,"theme":"","loginonstart":false,"loginoverlaycolor":"none","show_connection_error":true,"customStyles":""}');

-- [general] Timestamp (milliseconds since epoch)
INSERT INTO dbo.general ([name], [value]) VALUES (N'timestamp', CAST(CAST(DATEDIFF(s, '1970-01-01', GETUTCDATE()) AS BIGINT) * 1000 AS NVARCHAR(50)));

-- [devices] FUXA built-in server device
INSERT INTO dbo.devices ([name], [value], [connection], [cntid], [cntpwd])
VALUES (N'server', N'{"id":"0","name":"FUXA","type":"FuxaServer","enabled":true,"property":{}}', NULL, NULL, NULL);

-- [views] Default blank MainView (1024x768, empty SVG canvas)
-- NOTE: The `name` column stores view.id (not view.name)
INSERT INTO dbo.views ([name], [value]) VALUES (N'v_main', N'{"id":"v_main","name":"MainView","type":"svg","profile":{"width":1024,"height":768,"bkcolor":"#ffffffff","margin":10,"align":"topCenter","gridType":"Fixed","viewRenderDelay":0},"items":{},"variables":{},"svgcontent":""}');

-- [users] Default admin account (password: 123456)
-- NOTE: Replace the bcrypt hash if you want a different password
INSERT INTO dbo.users ([username], [fullname], [password], [groups], [info])
VALUES (N'admin', N'Administrator Account', N'$2a$10$Cj7efR0bMNE3cMjZYZ0GAOfxslG809jrSTlCiVtVc0QRhn9Cbg69S', -1, NULL);
GO

-- ============================================================
-- Verification query
-- ============================================================
-- SELECT 'general' AS tbl, COUNT(*) AS rows FROM general
-- UNION ALL SELECT 'devices', COUNT(*) FROM devices
-- UNION ALL SELECT 'views', COUNT(*) FROM views
-- UNION ALL SELECT 'users', COUNT(*) FROM users;
