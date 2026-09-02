-- ============================================================
-- MySQL 结果表（Flink JDBC Sink 目标）
-- 数据库: taobao_realtime
-- 用户: flink / flink123
-- ============================================================

CREATE DATABASE IF NOT EXISTS taobao_realtime;
USE taobao_realtime;

CREATE TABLE IF NOT EXISTS dws_category_sales (
  category VARCHAR(50),
  window_end DATETIME,
  total_sales DOUBLE,
  order_count BIGINT,
  PRIMARY KEY (category, window_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
