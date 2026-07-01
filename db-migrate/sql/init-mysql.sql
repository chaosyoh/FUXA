-- ============================================================
-- FUXA Project Initialization Script for MySQL
-- 
-- This script creates all FUXA database tables and inserts
-- the minimum initial data required for a new project.
--
-- Usage:
--   mysql -u root -p fuxa < init-mysql.sql
--
-- Note: All tables are created in a single database.
--       In the original FUXA, data is split across multiple
--       SQLite files. Here they are consolidated into one MySQL database.
-- ============================================================

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================
-- 1. PROJECT TABLES (from project.fuxap.db)
-- ============================================================

-- general: project-level key-value settings (version, layout, etc.)
DROP TABLE IF EXISTS `general`;
CREATE TABLE `general` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- views: HMI view definitions (SVG canvas, gauges, etc.)
DROP TABLE IF EXISTS `views`;
CREATE TABLE `views` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- devices: device configurations (server, PLCs, sensors, etc.)
DROP TABLE IF EXISTS `devices`;
CREATE TABLE `devices` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  `connection` TEXT,
  `cntid` VARCHAR(255),
  `cntpwd` VARCHAR(255),
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- devicesSecurity: device security/certificate settings
DROP TABLE IF EXISTS `devicesSecurity`;
CREATE TABLE `devicesSecurity` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- texts: multilingual text definitions
DROP TABLE IF EXISTS `texts`;
CREATE TABLE `texts` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- alarms: alarm configuration definitions
DROP TABLE IF EXISTS `alarms`;
CREATE TABLE `alarms` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- notifications: notification configuration definitions
DROP TABLE IF EXISTS `notifications`;
CREATE TABLE `notifications` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- scripts: user-defined scripts
DROP TABLE IF EXISTS `scripts`;
CREATE TABLE `scripts` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- reports: report configurations
DROP TABLE IF EXISTS `reports`;
CREATE TABLE `reports` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- locations: maps locations
DROP TABLE IF EXISTS `locations`;
CREATE TABLE `locations` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- 2. USER TABLES (from users.fuxap.db)
-- ============================================================

-- users: user accounts
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `username` VARCHAR(255) NOT NULL,
  `fullname` VARCHAR(255),
  `password` VARCHAR(255),
  `groups` INT,
  `info` TEXT,
  PRIMARY KEY (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- roles: user role definitions
DROP TABLE IF EXISTS `roles`;
CREATE TABLE `roles` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- 3. API KEYS TABLE (from apikeys.fuxap.db)
-- ============================================================

DROP TABLE IF EXISTS `apikeys`;
CREATE TABLE `apikeys` (
  `name` VARCHAR(255) NOT NULL,
  `value` LONGTEXT,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- 4. ALARM HISTORY TABLES (from alarms.fuxap.db)
-- ============================================================

-- alarms_runtime: current alarm states (separate from alarm config)
DROP TABLE IF EXISTS `alarms_runtime`;
CREATE TABLE `alarms_runtime` (
  `nametype` VARCHAR(255) NOT NULL,
  `type` VARCHAR(100),
  `status` VARCHAR(100),
  `ontime` BIGINT,
  `offtime` BIGINT,
  `acktime` BIGINT,
  PRIMARY KEY (`nametype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- alarms_chronicle: alarm history log
DROP TABLE IF EXISTS `alarms_chronicle`;
CREATE TABLE `alarms_chronicle` (
  `Sn` INT NOT NULL AUTO_INCREMENT,
  `nametype` VARCHAR(255),
  `type` VARCHAR(100),
  `status` VARCHAR(100),
  `text` TEXT,
  `grp` VARCHAR(255),
  `ontime` BIGINT,
  `offtime` BIGINT,
  `acktime` BIGINT,
  `userack` VARCHAR(255),
  PRIMARY KEY (`Sn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- 5. NOTIFICATION HISTORY TABLES (from notifications.fuxap.db)
-- ============================================================

DROP TABLE IF EXISTS `notifications_chronicle`;
CREATE TABLE `notifications_chronicle` (
  `Sn` INT NOT NULL AUTO_INCREMENT,
  `id` VARCHAR(255),
  `name` VARCHAR(255),
  `type` VARCHAR(100),
  `receiver` VARCHAR(255),
  `text` TEXT,
  `notifytime` BIGINT,
  `notifytype` VARCHAR(100),
  PRIMARY KEY (`Sn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- 6. SCHEDULER TABLE (from scheduler.db)
-- ============================================================

DROP TABLE IF EXISTS `schedulers`;
CREATE TABLE `schedulers` (
  `id` VARCHAR(255) NOT NULL,
  `data` LONGTEXT NOT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- 7. CURRENT TAG READINGS TABLE (from currentTagReadings.db)
-- ============================================================

DROP TABLE IF EXISTS `currentValues`;
CREATE TABLE `currentValues` (
  `tagId` VARCHAR(255) NOT NULL,
  `deviceId` VARCHAR(255),
  `value` TEXT,
  PRIMARY KEY (`tagId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================
-- INITIALIZATION DATA
-- ============================================================

-- [general] Project version
INSERT INTO `general` (`name`, `value`) VALUES ('version', '"1.01"');

-- [general] Layout settings (navigation, header, etc.)
INSERT INTO `general` (`name`, `value`) VALUES ('layout', '{"autoresize":false,"start":"","navigation":{"mode":"item.navsmode-over","type":"item.navtype-icons-text-block","bkcolor":"#F4F5F7","fgcolor":"#1D1D1D"},"header":{"bkcolor":"#ffffff","fgcolor":"#000000","fontSize":13,"itemsAnchor":"left"},"showdev":true,"inputdialog":"false","hidenavigation":false,"theme":"","loginonstart":false,"loginoverlaycolor":"none","show_connection_error":true,"customStyles":""}');

-- [general] Mobile layout settings
INSERT INTO `general` (`name`, `value`) VALUES ('mobileLayout', '{"autoresize":false,"start":"","navigation":{"mode":"item.navsmode-over","type":"item.navtype-icons-text-block","bkcolor":"#F4F5F7","fgcolor":"#1D1D1D"},"header":{"bkcolor":"#ffffff","fgcolor":"#000000","fontSize":13,"itemsAnchor":"left"},"showdev":true,"inputdialog":"false","hidenavigation":false,"theme":"","loginonstart":false,"loginoverlaycolor":"none","show_connection_error":true,"customStyles":""}');

-- [general] Timestamp
INSERT INTO `general` (`name`, `value`) VALUES ('timestamp', UNIX_TIMESTAMP() * 1000);

-- [devices] FUXA built-in server device
INSERT INTO `devices` (`name`, `value`, `connection`, `cntid`, `cntpwd`)
VALUES ('server', '{"id":"0","name":"FUXA","type":"FuxaServer","enabled":true,"property":{}}', NULL, NULL, NULL);

-- [views] Default blank MainView (1024x768, empty SVG canvas)
-- NOTE: The `name` column stores view.id (not view.name)
INSERT INTO `views` (`name`, `value`) VALUES ('v_main', '{"id":"v_main","name":"MainView","type":"svg","profile":{"width":1024,"height":768,"bkcolor":"#ffffffff","margin":10,"align":"topCenter","gridType":"Fixed","viewRenderDelay":0},"items":{},"variables":{},"svgcontent":""}');

-- [users] Default admin account (password: 123456)
-- NOTE: Replace the bcrypt hash if you want a different password
INSERT INTO `users` (`username`, `fullname`, `password`, `groups`, `info`)
VALUES ('admin', 'Administrator Account', '$2a$10$Cj7efR0bMNE3cMjZYZ0GAOfxslG809jrSTlCiVtVc0QRhn9Cbg69S', -1, NULL);

-- ============================================================
-- Verification query
-- ============================================================
-- SELECT 'general' AS tbl, COUNT(*) AS rows FROM general
-- UNION ALL SELECT 'devices', COUNT(*) FROM devices
-- UNION ALL SELECT 'views', COUNT(*) FROM views
-- UNION ALL SELECT 'users', COUNT(*) FROM users;
