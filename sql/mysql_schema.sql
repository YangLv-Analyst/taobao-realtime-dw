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

-- ============================================================
-- 可视化层统一入口 VIEW（Superset 数据集只指向此 VIEW，不直连物理表）
--
-- 时区策略（踩坑后确定）：全链路存 UTC
--   生产者 datetime.utcnow() -> Flink JVM -Duser.timezone=UTC
--   -> JDBC serverTimezone=UTC -> MySQL DATETIME(UTC)
--   展示层交给 Superset 的 DISPLAY_TIMEZONE = 'Asia/Shanghai' 自动 +8
--
-- 因此本 VIEW 必须是「直通」，绝不能再写 DATE_ADD(window_end, INTERVAL 8 HOUR)，
-- 否则与 DISPLAY_TIMEZONE 叠加成 +16 小时的双重转换。
-- 同理绝不要用 UPDATE 手动修正历史时间戳，需要清理时直接 TRUNCATE 让 Flink 重写。
-- ============================================================

CREATE OR REPLACE VIEW v_dws_category_sales AS
SELECT category,
       window_end,
       total_sales,
       order_count
FROM dws_category_sales;

-- 验证 VIEW 与物理表数据一致（两者 MAX 必须完全相同）
-- SELECT MAX(window_end) FROM dws_category_sales;
-- SELECT MAX(window_end) FROM v_dws_category_sales;
