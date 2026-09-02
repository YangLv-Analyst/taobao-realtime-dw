-- ============================================================
-- 淘宝实时数仓 Flink SQL 脚本
-- 环境: Flink 1.20.5 + Kafka 4.3.1 (KRaft) + MySQL 8.0
-- ============================================================

-- 1. Kafka 源表（带时间属性 + WATERMARK）
CREATE TABLE kafka_source (
  order_id BIGINT,
  customer_id STRING,
  gender STRING,
  age INT,
  category STRING,
  quantity INT,
  price DOUBLE,
  payment_method STRING,
  invoice_date STRING,
  event_time AS TO_TIMESTAMP(invoice_date),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'taobao_orders',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- 2. Kafka 聚合结果输出表
CREATE TABLE kafka_dws_sink (
  category STRING,
  window_end TIMESTAMP(3),
  total_sales DOUBLE,
  order_count BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'taobao_dws_category_sales',
  'properties.bootstrap.servers' = '127.0.0.1:9092',
  'format' = 'json'
);

-- 3. MySQL 结果表
CREATE TABLE mysql_dws_sink (
  category STRING,
  window_end TIMESTAMP(3),
  total_sales DOUBLE,
  order_count BIGINT,
  PRIMARY KEY (category, window_end) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/taobao_realtime?useSSL=false&allowPublicKeyRetrieval=true',
  'table-name' = 'dws_category_sales',
  'username' = 'flink',
  'password' = 'flink123',
  'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 4. 实时清洗 + 聚合写入 Kafka
INSERT INTO kafka_dws_sink
SELECT
  category,
  TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
  SUM(price * quantity) AS total_sales,
  COUNT(*) AS order_count
FROM kafka_source
WHERE quantity > 0 AND price < 10000
GROUP BY
  category,
  TUMBLE(event_time, INTERVAL '1' MINUTE);

-- 5. 写入 MySQL（多 Sink 架构）
INSERT INTO mysql_dws_sink
SELECT
  category,
  TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
  SUM(price * quantity) AS total_sales,
  COUNT(*) AS order_count
FROM kafka_source
WHERE quantity > 0 AND price < 10000
GROUP BY
  category,
  TUMBLE(event_time, INTERVAL '1' MINUTE);
