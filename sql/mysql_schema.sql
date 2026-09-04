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
-- 时区策略（2026-09-04 代码级取证后定案）：存储层全链路 UTC，+8 只做在 VIEW 里
--   生产者 datetime.utcnow() -> Flink JVM -Duser.timezone=UTC
--   -> JDBC serverTimezone=UTC -> MySQL DATETIME 存 UTC 原值（如 04:58）
--   -> 本 VIEW 做 DATE_ADD(window_end, INTERVAL 8 HOUR) 输出北京时间（12:58）
--
-- 为什么 +8 必须写在 VIEW，而不是靠 Superset 的 DISPLAY_TIMEZONE：
--   1) MySQL 的 DATETIME 不携带任何时区元数据，Superset 拿到的是 naive 值；
--   2) 折线图 X 轴默认时间格式 smart_date 在前端注册时【不传 useLocalTime】，
--      默认 false -> 走 getUTC* 分支 -> 原样渲染库里的 naive 值，不做本地化；
--   3) DISPLAY_TIMEZONE 与 JDBC URI 上的时区参数，都只对「已带时区的值」做换算，
--      对 naive DATETIME 的图表时间轴无效。
--   => VIEW +8 是唯一生效的那一次转换，且必须恰好只有一次（不要再叠 local! 前缀）。
--
-- VIEW +8 还有一个必要副作用：Superset 的相对时间过滤器（如 1 hour ago : now）
--   用【服务器本地时间】（WSL 继承 Windows = 北京时间）计算边界，VIEW 输出北京时间后
--   两边口径一致，滚动窗口才能真正查到数据；直通 VIEW 会让过滤器永远落空。
--
-- 同理绝不要用 UPDATE 手动修正历史时间戳，需要清理时直接 TRUNCATE 让 Flink 重写。
-- ============================================================

CREATE OR REPLACE VIEW v_dws_category_sales AS
SELECT category,
       DATE_ADD(window_end, INTERVAL 8 HOUR) AS window_end,
       total_sales,
       order_count
FROM dws_category_sales;

-- 验证 VIEW 正确 +8：VIEW 的 MAX 应等于物理表 MAX + 8 小时，且约等于 NOW()
-- SELECT MAX(window_end) AS max_utc, NOW() AS now_bj FROM dws_category_sales;
-- SELECT MAX(window_end) AS max_bj,  NOW() AS now_bj FROM v_dws_category_sales;
