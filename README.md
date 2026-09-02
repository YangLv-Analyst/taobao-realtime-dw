# 淘宝实时数仓 (taobao-realtime-dw)

> 基于 Flink + Kafka + MySQL + Superset 的实时数据仓库项目
> 作者：杨铝 | 26 届本科应届生 | 大数据学习项目

---

## 一、项目背景

本项目是离线数仓（taobao-dw-project）的实时化升级。离线数仓使用 Hadoop + Hive + Spark 做 T+1 批处理，本项目在此基础上引入 Kafka + Flink 实现**分钟级实时 DWS 层**，最终通过 Superset 展示实时销售大屏。

## 二、架构概览

```
Python 生产者 (order_producer.py)
    | 模拟淘宝订单（泊松到达 + 时段系数 + 品类加权 + 二八定律 + 1% 脏数据）
    v
Kafka (taobao_orders)                    <-- 原始订单流
    |
    v
Flink SQL (TUMBLE 窗口聚合 + 数据清洗)    <-- 每分钟刷新
    |
    +---> Kafka (taobao_dws_category_sales)   [Sink 1: 聚合结果流]
    +---> MySQL (dws_category_sales)          [Sink 2: 结果持久化]
    +---> Superset (实时销售大屏)              [Sink 3: BI 可视化]
```

### 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Python | 3.12 | 订单仿真生产者 |
| Kafka | 4.3.1 (KRaft) | 消息队列 |
| Flink | 1.20.5 | 实时计算引擎 |
| MySQL | 8.0 | 结果存储 |
| Superset | 6.x | BI 可视化 |

### 运行环境

- Windows 11 + WSL2 Ubuntu 22.04
- 单节点集群（Hadoop 3.3.6 / Hive 3.1.3 / Spark 3.5.1 已部署）
- Java 17（Flink 要求）+ Java 8（Hadoop/Spark 要求），双版本隔离

---

## 三、从零搭建全流程

### 阶段 1：Kafka 部署

#### 1.1 下载与安装

```bash
# 先侦察真实文件名（禁止猜测！）
curl https://archive.apache.org/dist/kafka/

# 下载（以实际版本为准）
wget https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz
sudo mv kafka_2.13-4.3.1 /opt/kafka
```

#### 1.2 KRaft 模式配置

Kafka 4.3 已删除 ZooKeeper，使用 KRaft 模式：

```bash
# 格式化集群（只需一次）
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```

#### 1.3 配置 listeners（关键！）

```bash
# 编辑 config/server.properties，末尾追加：
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
```

> **坑 1：CONTROLLER 耳朵缺失**
> - 现象：启动后 jps 无 Kafka 进程
> - 原因：`listeners` 只写了 PLAINTEXT，缺少 CONTROLLER 耳朵
> - 解决：`listeners` 必须同时包含 PLAINTEXT 和 CONTROLLER

#### 1.4 启动 Kafka

```bash
bin/kafka-server-start.sh config/server.properties &
jps  # 确认看到 Kafka 进程
```

#### 1.5 创建主题

```bash
bin/kafka-topics.sh --create --topic taobao_orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

### 阶段 2：Python 生产者

#### 2.1 创建虚拟环境

```bash
cd ~/taobao-realtime-dw
python3 -m venv .venv
source .venv/bin/activate
pip install kafka-python
```

#### 2.2 生产者核心设计

`order_producer.py` 包含 6 大仿真参数：

| 参数 | 实现 | 业务含义 |
|------|------|----------|
| 品类加权 | `random.choices(weights=[31,16,15,...])` | Clothing 占 31%，贴合电商真实分布 |
| 品类锚定价格 | 每个品类有独立价格区间 | 电子产品贵、食品便宜 |
| 时段波峰 | `0.2 + 1.8 * max(0, 1 - abs(hour-21)/9)` | 凌晨冷清、晚 9 点爆单 |
| 二八用户 | 60% 订单来自头部 100 用户 | 高频买家贡献大部分 GMV |
| 1% 脏数据 | 0.5% 负数量 + 0.5% 价格离群 | 给下游清洗留实战素材 |
| 泊松到达 | `random.expovariate(hour_factor())` | 订单间隔服从指数分布 |

#### 2.3 启动生产者

```bash
python order_producer.py
```

> **坑 2：IPv6 localhost 陷阱**
> - 现象：`KafkaTimeoutError: Unable to bootstrap from localhost:9092`
> - 原因：Windows 下 `localhost` 解析到 IPv6 `::1`，Kafka 只绑 IPv4
> - 解决：客户端和 `advertised.listeners` 都用 `127.0.0.1`，不用 `localhost`

---

### 阶段 3：Flink 部署

#### 3.1 下载与安装

```bash
# 侦察真实文件名
curl https://archive.apache.org/dist/flink/

wget https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz
tar -xzf flink-1.20.5-bin-scala_2.12.tgz
sudo mv flink-1.20.5 /opt/flink
```

#### 3.2 Java 17 隔离

```bash
# WSL 默认 Java 8，Flink 1.20 需要 Java 17
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
java -version  # 确认 17.x
```

#### 3.3 修改 REST 端口

```bash
# Spark Master 占了 8081，Flink 改到 8090
echo "rest.port: 8090" >> /opt/flink/conf/config.yaml
```

> **坑 3：配置文件改名**
> - 现象：`sed: can't read conf/flink-conf.yaml: No such file`
> - 原因：Flink 1.20+ 配置文件从 `flink-conf.yaml` 改为 `config.yaml`

#### 3.4 启动 Flink

```bash
cd /opt/flink
bin/start-cluster.sh
jps  # 确认 StandaloneSessionClusterEntrypoint + TaskManagerRunner
```

浏览器访问 `http://localhost:8090` 确认 Web UI 正常。

#### 3.5 下载连接器

```bash
# Kafka 连接器
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
sudo cp flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/

# MySQL JDBC 驱动
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
sudo cp mysql-connector-j-8.0.33.jar /opt/flink/lib/

# JDBC 连接器（先侦察版本！）
curl https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/
# 确认最新版本匹配 Flink 1.20
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.4.0-1.20/flink-connector-jdbc-3.4.0-1.20.jar
sudo cp flink-connector-jdbc-3.4.0-1.20.jar /opt/flink/lib/

# 重启 Flink 使连接器生效
bin/stop-cluster.sh
bin/start-cluster.sh
```

> **坑 4：下载前必须侦察**
> - 现象：多次 404 Not Found
> - 原因：文件名/版本号猜错（如 JDBC 连接器没有 `apache-` 前缀，MySQL 驱动 8.0.31+ 改名）
> - 解决：**永远先 `curl` 列目录确认真实文件名，再下载**

---

### 阶段 4：Flink SQL 实时计算

#### 4.1 启动 SQL Client

```bash
cd /opt/flink
bin/sql-client.sh
```

#### 4.2 创建源表（带时间属性）

```sql
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
```

> **坑 5：TUMBLE 窗口类型错误**
> - 现象：`Cannot apply '$TUMBLE' to arguments of type '$TUMBLE(<VARCHAR>, <INTERVAL MINUTE>)'`
> - 原因：`invoice_date` 是 STRING，TUMBLE 需要时间属性列
> - 解决：用计算列 `event_time AS TO_TIMESTAMP(invoice_date)` + `WATERMARK`

> **坑 6：普通 TIMESTAMP 不够**
> - 现象：`Window aggregate can only be defined over a time attribute column, but TIMESTAMP(6) encountered`
> - 原因：普通 TIMESTAMP 不是时间属性列，必须带 WATERMARK
> - 解决：`WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`

#### 4.3 创建 Sink 表 + 执行聚合

```sql
-- Kafka Sink
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

-- MySQL Sink
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

-- 执行聚合（同时写 Kafka + MySQL）
INSERT INTO kafka_dws_sink
SELECT category,
  TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
  SUM(price * quantity) AS total_sales,
  COUNT(*) AS order_count
FROM kafka_source
WHERE quantity > 0 AND price < 10000
GROUP BY category, TUMBLE(event_time, INTERVAL '1' MINUTE);

INSERT INTO mysql_dws_sink
SELECT category,
  TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
  SUM(price * quantity) AS total_sales,
  COUNT(*) AS order_count
FROM kafka_source
WHERE quantity > 0 AND price < 10000
GROUP BY category, TUMBLE(event_time, INTERVAL '1' MINUTE);
```

> **坑 7：JDBC 连接器未加载**
> - 现象：`Could not find any factory for identifier 'jdbc'`
> - 原因：jar 放入了 lib/ 但 SQL Client 是重启前启动的，classpath 没更新
> - 解决：退出 SQL Client（QUIT），重新启动

> **坑 8：会话级表定义丢失**
> - 现象：重启 SQL Client 后 `Object 'kafka_source' not found`
> - 原因：Flink SQL Client 的表定义是会话级的，重启后丢失
> - 解决：每次重启后重新 CREATE TABLE

> **坑 9：CANCEL JOB 语法不支持**
> - 现象：`Non-query expression encountered in illegal context`
> - 原因：Flink SQL Client 不支持 `CANCEL JOB` SQL 语法
> - 解决：用 `curl -X PATCH "http://localhost:8090/jobs/<id>?mode=cancel"` REST API

---

### 阶段 5：MySQL 结果存储

```bash
# 启动 MySQL
sudo service mysql start

# 创建数据库和用户
sudo mysql
> CREATE USER 'flink'@'localhost' IDENTIFIED BY 'flink123';
> CREATE DATABASE taobao_realtime;
> GRANT ALL ON taobao_realtime.* TO 'flink'@'localhost';
> FLUSH PRIVILEGES;

# 建表
USE taobao_realtime;
CREATE TABLE dws_category_sales (
  category VARCHAR(50),
  window_end DATETIME,
  total_sales DOUBLE,
  order_count BIGINT,
  PRIMARY KEY (category, window_end)
);

# 验证数据（等 1-2 分钟让窗口关闭）
mysql -u flink -pflink123 taobao_realtime -e "SELECT * FROM dws_category_sales ORDER BY window_end DESC LIMIT 10;"
```

> **坑 10：MySQL root 用户 auth_socket 认证**
> - 现象：`ERROR 1698 (28000): Access denied for user 'root'@'localhost'`
> - 原因：Ubuntu 下 MySQL root 默认用 auth_socket，不支持密码登录
> - 解决：用 `sudo mysql` 进入，创建普通用户 `flink`

---

### 阶段 6：Superset 可视化

#### 6.1 安装与启动

```bash
python3 -m venv ~/superset-env
source ~/superset-env/bin/activate
pip install apache-superset
```

#### 6.2 配置文件

```bash
cat > ~/superset_config.py << 'EOF'
import os
SECRET_KEY = 'taobao-realtime-dw-2026-secret-key'
SQLALCHEMY_DATABASE_URI = 'sqlite:////home/yanglv/superset.db'
WTF_CSRF_ENABLED = False
EOF

export SUPERSET_CONFIG_PATH=~/superset_config.py
superset db upgrade
superset fab create-admin
superset init
superset run -p 8089 --with-threads
```

> **坑 11：SECRET_KEY 不安全拒绝启动**
> - 现象：`Refusing to start due to insecure SECRET_KEY`
> - 原因：Superset 检测到使用默认 SECRET_KEY
> - 解决：创建 `superset_config.py` 指定自定义 SECRET_KEY

> **坑 12：500 错误 - 权限表为空**
> - 现象：登录页 500，菜单入口全部消失
> - 原因：新数据库的权限表是空的，Admin 角色没有权限
> - 解决：执行 `superset init` 同步权限

#### 6.3 添加 MySQL 数据源

Superset 6.x 的 UI 变化很大，数据库管理入口不在 Settings 里：

```bash
# 通过 API 添加（绕过 UI 限制）
TOKEN=$(curl -s -X POST "http://localhost:8089/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"YANGLV","password":"admin","provider":"db","refresh":true}' \
  | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

curl -X POST "http://localhost:8089/api/v1/database/" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"database_name":"taobao_realtime","sqlalchemy_uri":"mysql+pymysql://flink:flink123@localhost:3306/taobao_realtime"}'
```

> **坑 13：mysqlclient 编译失败**
> - 现象：`ERROR: Failed to build 'mysqlclient'`
> - 原因：需要 C 编译器 + MySQL 开发库，且 apt 包索引过期导致 404
> - 解决：先 `sudo apt update`，再 `sudo apt install default-libmysqlclient-dev build-essential pkg-config`，最后 `pip install mysqlclient==2.2.0`

> **坑 14：No module named 'MySQLdb'**
> - 现象：Superset 查询时报 `No module named 'MySQLdb'`
> - 原因：Superset 内部仍尝试加载 mysqlclient 模块
> - 解决：安装 mysqlclient 2.2.0（需要系统依赖 + 编译）

> **坑 15：JWT 认证 + 用户名大小写**
> - 现象：API 返回 `Not authorized`
> - 原因：`superset fab create-admin` 创建的用户名是大写 `YANGLV`，不是 `admin`
> - 解决：API 登录时用正确的用户名 `YANGLV`

> **坑 16：CSRF 保护阻止 API**
> - 现象：`CSRFError: 400 Bad Request: The CSRF token is missing`
> - 原因：Flask-WTF 的 CSRF 保护阻止了 curl 请求
> - 解决：配置文件中加 `WTF_CSRF_ENABLED = False`

#### 6.4 创建图表

1. **柱状图**（品类销售额对比）：X 轴 `category`，Y 轴 `SUM(total_sales)`
2. **折线图**（销售趋势）：X 轴 `window_end`，Y 轴 `SUM(total_sales)`，Group By `category`，Time Grain 设为 `Minute`
3. **时间过滤器**：Filter `window_end`，选 `Last day`

> **坑 17：折线图只有两行**
> - 现象：折线图只显示 2 个数据点
> - 原因：Time Grain 默认是 `Day`，按天聚合
> - 解决：左侧 Time 区域 → Time Grain 改为 `Minute`

> **坑 18：category 误放 Metrics**
> - 现象：弹窗要求选聚合方式
> - 原因：category 是维度，不应放 Metrics 区域
> - 解决：category 拖到 Group By/Dimensions，不选聚合

---

## 四、踩坑总结（Top 10）

| # | 坑 | 根因 | 解决 |
|---|-----|------|------|
| 1 | Kafka 启动无进程 | listeners 缺 CONTROLLER 耳朵 | 加 `CONTROLLER://localhost:9093` |
| 2 | KafkaTimeoutError | Windows localhost 解析到 IPv6 | 用 `127.0.0.1` 替代 `localhost` |
| 3 | Flink 配置文件找不到 | 1.20+ 改名 config.yaml | 用 `conf/config.yaml` |
| 4 | 下载 404 | 文件名/版本号猜错 | **先 curl 侦察再下载** |
| 5 | TUMBLE 类型错误 | STRING 不能做窗口 | 计算列 `TO_TIMESTAMP` + WATERMARK |
| 6 | JDBC 连接器找不到 | SQL Client 是旧 classpath | 重启 SQL Client |
| 7 | MySQL root 登不了 | auth_socket 认证 | 用 `sudo mysql` + 创建普通用户 |
| 8 | Superset 500 | 权限表为空 | `superset init` |
| 9 | mysqlclient 编译失败 | 缺系统依赖 + apt 过期 | `apt update` + 装开发库 |
| 10 | API 认证失败 | 用户名大写 + CSRF | 用正确用户名 + 禁用 CSRF |

---

## 五、项目结构

```
taobao-realtime-dw/
├── order_producer.py      # 高仿真订单生产者（6 大仿真参数）
├── requirements.txt       # Python 依赖
├── sql/
│   ├── flink_realtime.sql # Flink SQL 脚本（源表 + 双 Sink + 聚合）
│   ├── mysql_schema.sql   # MySQL 建表脚本
│   ── kafka_topics.sh    # Kafka 主题创建命令
├── .gitignore
── README.md              # 本文档
```

---

## 六、运行效果

### 实时销售大屏

- **柱状图**：8 个品类销售额对比（Electronics > Home > Clothing 前三）
- **折线图**：每分钟销售趋势，晚 21 点明显爆单峰值
- **时间过滤器**：可按 Last hour / Last day / Last 7 days 筛选

### 数据样例

```
+-------------+---------------------+-------------+-------------+
| category    | window_end          | total_sales | order_count |
+-------------+---------------------+-------------+-------------+
| Electronics | 2026-09-03 02:13:00 |    34865.51 |          14 |
| Home        | 2026-09-03 02:13:00 |    41034.66 |          16 |
| Clothing    | 2026-09-03 02:13:00 |    30302.29 |          37 |
+-------------+---------------------+-------------+-------------+
```

---

## 七、后续优化方向

1. **更多指标**：客单价、转化率、UV/PV、复购率
2. **实时告警**：某品类销售额突降 → 钉钉/邮件通知
3. **OLAP 升级**：MySQL → Doris/ClickHouse（生产级查询性能）
4. **维度扩展**：按城市、年龄段、支付方式多维分析
5. **CEP 复杂事件处理**：检测异常订单模式（刷单、欺诈）

---

## 八、关联项目

- 离线数仓：[taobao-dw-project](https://github.com/YangLv-Analyst/taobao-dw-project)
- 实时数仓：[taobao-realtime-dw](https://github.com/YangLv-Analyst/taobao-realtime-dw)（本仓库）
