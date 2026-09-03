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

# 展示时区：MySQL 存 UTC，Superset 渲染时自动 +8 成北京时间
DISPLAY_TIMEZONE = 'Asia/Shanghai'

# 实时数仓必须禁用全部 4 类缓存通道，缺一个都会导致图表显示旧数据
CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}
DATA_CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}
FILTER_STATE_CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}
EXPLORE_FORM_CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}
EOF
```

> ⚠️ **`SQLALCHEMY_ENGINE_OPTIONS = {"server_side_cursors": True}` 绝对不能加**，
> 元数据库是 SQLite，不支持服务端游标，加了直接 `Failed to create app`（见坑 22）。

#### 6.2.1 让配置文件真正被加载（最关键的一步）

Superset **不会自动读取** `~/superset_config.py`。只在某个终端 `export` 过一次，
新开终端就失效，所有配置项（时区、缓存、SECRET_KEY）全部空转：

```bash
# 写进 ~/.bashrc 永久生效
echo 'export SUPERSET_CONFIG_PATH=$HOME/superset_config.py' >> ~/.bashrc
source ~/.bashrc

# 初始化 + 启动
superset db upgrade
superset fab create-admin
superset init
superset run -p 8089 --with-threads
```

**生效判定标准**（启动日志必须同时满足两条）：

```
Loaded your LOCAL configuration at [/home/yanglv/superset_config.py]   <-- 必须有这行
（且不出现）A Default SECRET_KEY was detected                            <-- 必须没这行
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

#### 6.4 创建图表（最终版：4 个图表）

| 图表名 | 类型 | 关键配置 | 作用 |
|--------|------|---------|------|
| 数据最新时间 | Big Number（或 Table） | Metric 用 Custom SQL `MAX(window_end)` | 一眼证明数据新鲜度 |
| 累计销售额 | Big Number with Trendline | Metric `SUM(total_sales)`，Time Grain `Minute` | 大数字 + 迷你趋势线 |
| 品类销售趋势 | Line Chart | X 轴 `window_end`，Time Grain `Minute`，Metrics `SUM(total_sales)`，Dimensions `category` | 分钟级实时趋势 |
| 品类销售额对比 | Table Chart | 列 `category` / `window_end` / `SUM(total_sales)` / `SUM(order_count)` | 明细核对 |

**所有图表的统一硬性要求**（每一条都对应一个踩过的坑）：

- 数据集一律指向 VIEW `v_dws_category_sales`，不直连物理表
- Filters 一律 **No filter**（见坑 27）
- Row limit 一律 **50000**（见坑 26）
- Contribution Mode 一律 **None**（见坑 28）
- Big Number 显示日期若变成 Unix 时间戳或 `NaN`，改用 Table 类型渲染

> **坑 17：折线图只有两行**
> - 现象：折线图只显示 2 个数据点
> - 原因：Time Grain 默认是 `Day`，按天聚合
> - 解决：左侧 Time 区域 → Time Grain 改为 `Minute`

> **坑 18：category 误放 Metrics**
> - 现象：弹窗要求选聚合方式
> - 原因：category 是维度，不应放 Metrics 区域
> - 解决：category 拖到 Group By/Dimensions，不选聚合

#### 6.5 时区攻坚实录

这是整个项目耗时最长的一段排查。核心矛盾是：**MySQL 的 `DATETIME` 类型不携带任何时区元数据**，
所以同一个 `2026-09-03 05:16:00`，生产者、Flink、MySQL、Superset 四方各有各的解释。

**最终确定的全链路 UTC 策略**：

| 层级 | 配置 | 结果 |
|------|------|------|
| Python 生产者 | `datetime.utcnow().strftime(...)` | 生成 UTC 字符串 |
| Flink JVM | `env.java.opts: -Duser.timezone=UTC` | 窗口时间按 UTC 计算 |
| JDBC Sink | URL 加 `serverTimezone=UTC` | 写入不被驱动二次转换 |
| MySQL | `DATETIME` 存 UTC 原值 | `05:16` |
| VIEW | **直通，不做任何 DATE_ADD** | `05:16` |
| Superset | `DISPLAY_TIMEZONE = 'Asia/Shanghai'` | 渲染成 `13:16` ✅ |

**排查过程中走过的弯路**：

| 尝试方案 | MySQL 原值 | Superset 显示 | 结论 |
|---------|-----------|--------------|------|
| VIEW `DATE_ADD(+8)` + `DISPLAY_TIMEZONE` | 04:14 | 08:14 AM | ❌ 双重转换 |
| VIEW `DATE_SUB(-8)` + `DISPLAY_TIMEZONE` | 04:16 | 前一天 20:16 | ❌ 反向偏移 |
| 手动 `UPDATE` 历史数据 +8 | — | MAX 时间比当前还快 8h | ❌ 污染数据 |
| VIEW 直通 + `DISPLAY_TIMEZONE` | 05:16 | 13:16 | ✅ 正解 |

> **坑 19：全链路时区不一致**
> - 现象：Superset tooltip 时间比北京时间慢 8 小时
> - 原因：生产者用 `datetime.now()` 生成北京时间字符串，Flink JVM 却按 UTC 解释
> - 解决：生产者改 `datetime.utcnow()`，Flink JVM 加 `-Duser.timezone=UTC`，JDBC URL 加 `serverTimezone=UTC`

> **坑 20：VIEW 与 DISPLAY_TIMEZONE 双重转换**
> - 现象：tooltip 显示时间比真实时间快 4~16 小时，随配置组合漂移
> - 原因：VIEW 已 `DATE_ADD(+8)`，Superset 又按 `DISPLAY_TIMEZONE` +8
> - 解决：**二选一，只能有一个 +8**。最终选 VIEW 直通 + `DISPLAY_TIMEZONE = 'Asia/Shanghai'`

> **坑 21：用 UPDATE 修时区导致数据污染**
> - 现象：VIEW 查出 `MAX(window_end)` 比当前时间还快 8 小时
> - 原因：先手动 `UPDATE ... DATE_ADD(+8)` 改历史数据，之后 VIEW 又 +8，同一批数据被转了两次
> - 解决：`TRUNCATE TABLE dws_category_sales` 清空，让 Flink 重新写入干净数据
> - **教训：时区是展示层问题，永远不要动存储层的值**

> **坑 22：SQLite 不支持 server side cursors**
> - 现象：`sqlalchemy.exc.ArgumentError: Dialect SQLiteDialect_pysqlite does not support server side cursors` → `Failed to create app`
> - 原因：配置里写了 `SQLALCHEMY_ENGINE_OPTIONS = {"server_side_cursors": True}`，这是 MySQL/PostgreSQL 才有的选项
> - 解决：删掉该行；元数据库是 SQLite 时不要配任何引擎级游标选项

> **坑 23：改动 SECRET_KEY 后全线 Invalid decryption key**
> - 现象：所有图表、数据库连接列表都报 `Invalid decryption key`
> - 原因：数据库密码是用旧 SECRET_KEY 加密存在 SQLite 里的，新 key 解不开
> - 解决：重新添加 MySQL 连接，并绕过 ORM 直接清理旧记录（ORM 查询本身也会触发解密报错）：
>   ```bash
>   python3 -c "
>   import sqlite3
>   conn = sqlite3.connect('/home/yanglv/superset.db')
>   conn.execute('UPDATE tables SET database_id = 2 WHERE database_id = 1')
>   conn.execute('DELETE FROM dbs WHERE id = 1')
>   conn.commit()
>   "
>   ```

#### 6.6 实时性攻坚实录

时区修好之后出现的第二个问题：**MySQL 数据明明是实时的，图表却停在 1.5 小时前**。

> **坑 24：`superset_config.py` 根本没被加载（本项目最大的坑）**
> - 现象：`DISPLAY_TIMEZONE`、`NullCache` 改了全无效果；`superset shell` 报 `A Default SECRET_KEY was detected` / `Refusing to start due to insecure SECRET_KEY`
> - 原因：Superset 不会自动读取 `~/superset_config.py`，必须靠环境变量 `SUPERSET_CONFIG_PATH` 指定；只在某个终端 `export` 过，新开终端即失效
> - 解决：写进 `~/.bashrc` 永久生效
>   ```bash
>   echo 'export SUPERSET_CONFIG_PATH=$HOME/superset_config.py' >> ~/.bashrc && source ~/.bashrc
>   ```
> - 验证：启动日志出现 `Loaded your LOCAL configuration at [...]` 且无 SECRET_KEY 警告

> **坑 25：只禁 2 类缓存不够，图表仍滞后**
> - 现象：MySQL `MAX(window_end)` = 当前时间，图表却停在 1.5 小时前；点 Update chart 也没用
> - 原因：Superset 有 4 类缓存通道，只设 `CACHE_CONFIG` / `DATA_CACHE_CONFIG` 时，过滤器与探索表单缓存仍在返回旧结果
> - 解决：4 个全部设为 `NullCache`（见 6.2 配置）

> **坑 26：Row limit 1000 截断最新数据**
> - 现象：跑了约 2.4 小时后图表又开始"变慢"，最新时间点不再前进
> - 原因：7 个品类 × 每分钟 1 行 = 7 行/分钟，默认 Row limit 1000 只够 `1000 ÷ 7 ≈ 142 分钟`
> - 解决：Row limit 改 **50000**（够撑约 5 天）；长期方案见"七、后续优化方向"

> **坑 27：相对时间过滤器查不到数据**
> - 现象：设 `1 hour ago → now` 后图表 No data，生成的条件是 `window_end >= 2026-09-03T13:23:48`
> - 原因：Superset 用**服务器本地时间（WSL 继承 Windows = 北京时间）**计算相对范围，而表里存的是 UTC 值，两边差 8 小时，条件永远匹配不上
> - 解决：保持 **No filter**；若要启用滚动窗口，需让 VIEW 输出北京时间并把 `DISPLAY_TIMEZONE` 改为 `UTC`，使过滤器与数据处于同一时区基准

> **坑 28：Contribution Mode 被设成 Row**
> - 现象：Y 轴变成 30% / 40% / 50%，看不到真实销售额，折线挤成一团
> - 原因：Contribution Mode = Row 会按行归一化成百分比
> - 解决：改成 **None**

**实时性三重校验法**（排查时靠这三条定位问题，比反复改配置有效得多）：

```bash
# 校验 1：底层数据是否实时（latest_utc + 8h 应约等于 beijing_now）
mysql -u flink -pflink123 taobao_realtime -e \
  "SELECT MAX(window_end) AS latest_utc, NOW() AS beijing_now, COUNT(*) AS total FROM dws_category_sales;"

# 校验 2：VIEW 是否直通（MIN/MAX/COUNT 应与物理表完全一致）
mysql -u flink -pflink123 taobao_realtime -e \
  "SELECT MIN(window_end), MAX(window_end), COUNT(*) FROM v_dws_category_sales;"

# 校验 3：MySQL 服务器时区（SYSTEM 表示继承 Windows 时区 = UTC+8）
mysql -u flink -pflink123 -e "SELECT @@global.time_zone, NOW(), UTC_TIMESTAMP();"
```

再配合前端两个数字交叉验证：

- 图表底部 **`Last queried at`** = 当前时间 → 证明缓存已禁用，真的回源查询了
- **行数反推**：`Results 行数 ÷ 品类数 ≈ 时间跨度分钟数`
  实测 506 行 ÷ 7 品类 ≈ 72 分钟，正好对应 `13:32 → 14:44`，与 MySQL 完全吻合

**端到端延迟测算**：

| 环节 | 延迟 | 说明 |
|------|------|------|
| 生产者 → Kafka | < 100 ms | 同步 send |
| Kafka → Flink 窗口关闭 | ≤ 60 s | TUMBLE 1 分钟窗口的固有延迟，架构下限 |
| Flink → MySQL | < 1 s | JDBC upsert |
| MySQL → Superset 查询 | < 50 ms | 主键索引，千行级数据 |
| 仪表盘自动轮询 | ≤ 60 s | auto-refresh interval |
| **合计** | **约 1~2 分钟** | 达到分钟级实时看板标准 |

#### 6.7 仪表盘组装与前端问题

**布局建议**：

```
+------------------------------------------+
| [数据最新时间]        [累计销售额]         |  <- 两个大数字并排，占顶部一小行
+------------------------------------------+
| [品类销售趋势 折线图]（占满整行）           |
+------------------------------------------+
| [品类销售额对比 表格]（占满整行）           |
+------------------------------------------+
```

**自动刷新**（只有仪表盘浏览模式有，图表编辑页永远不自动刷新）：

1. 顶部 `Dashboards` → 点仪表盘**蓝色标题**进入浏览模式
2. 右上角 `⋮` → **Set auto-refresh interval** → 选 `1 minute`
3. 生效标志：右上角出现 `Refreshing every 1 minute`，保持标签页开着别关

> **坑 29：图表编辑页不会自动前进，误以为项目失败**
> - 现象：折线图最新时间点长时间不动，怀疑数据链路断了
> - 原因：所有 BI 工具的图表都是**一次查询的快照**，不点 Update chart 就不会重新查询；不存在"再过几小时就追上"的机制
> - 解决：自动刷新只能配在**仪表盘**上；图表编辑页仅用于调试

> **坑 30：Save 按钮一直灰色**
> - 现象：仪表盘编辑模式下 Save 始终不可点
> - 原因：把仪表盘名字输进了右侧面板顶部的**图表搜索框**（那是用来筛选图表列表的）；切换排序方式也不算改动
> - 解决：真正的标题框在**画布顶部**，placeholder 是 `Title is required`；输入后 Save 立即变亮

> **坑 31：切换数据集后图表配置全丢**
> - 现象：Swap dataset 后 X 轴 / Metrics / Dimensions 全部清空，报 `Missing dataset`
> - 原因：Superset 切换数据集会重置图表配置
> - 解决：切换前记录配置；或改用 `Datasets` 里修改 Database 指向的方式，避免 Swap

> **坑 32：前端 NotFoundError removeChild**
> - 现象：`NotFoundError: Failed to execute 'removeChild' on 'Node'：被移除的节点不是该节点的子节点`
> - 原因：浏览器自动翻译插件直接改 DOM，与 React 虚拟 DOM 冲突；或重启 Superset 后前端状态过期
> - 解决：`Ctrl+Shift+R` 硬刷新 → 关闭"翻译此页"（选"永不翻译此网站"）→ 无痕窗口排除其他扩展

> **坑 33：MySQL 保留字做列别名**
> - 现象：`ERROR 1064 (42000) ... near 'current_time, COUNT(*)...'`
> - 原因：`current_time` 是 MySQL 保留字
> - 解决：别名换成 `now_time` / `beijing_now`

---

## 四、踩坑总结（33 条）

### 数据链路层 Top 10（坑 1~18 精选）

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

### 可视化层踩坑（坑 19~33）

| # | 坑 | 根因 | 解决 |
|---|-----|------|------|
| 19 | 图表时间慢 8 小时 | 生产者 `datetime.now()` 与 Flink UTC 混用 | 全链路统一 UTC |
| 20 | 时间快 4~16 小时漂移 | VIEW +8 与 DISPLAY_TIMEZONE +8 双重转换 | VIEW 直通，只保留一个 +8 |
| 21 | MAX 时间比当前还快 | 手动 UPDATE 修时区污染数据 | TRUNCATE 让 Flink 重写 |
| 22 | Superset 启动即崩 | SQLite 不支持 server_side_cursors | 删掉 ENGINE_OPTIONS |
| 23 | Invalid decryption key | 改了 SECRET_KEY，旧密码解不开 | 重连 + sqlite3 绕过 ORM 清库 |
| 24 | **配置全部空转** | **未设 `SUPERSET_CONFIG_PATH`，配置文件没被加载** | **写进 `~/.bashrc` 永久生效** |
| 25 | 图表滞后 1.5 小时 | 只禁了 2 类缓存，还有 2 类在缓存 | 4 类缓存全设 NullCache |
| 26 | 2.4 小时后又变慢 | Row limit 1000 只够 142 分钟 | Row limit 改 50000 |
| 27 | 相对时间过滤器 No data | 过滤器用北京时间，数据是 UTC | 保持 No filter |
| 28 | Y 轴变成百分比 | Contribution Mode = Row | 改成 None |
| 29 | 图表不会自己前进 | BI 图表是查询快照 | 仪表盘配 auto-refresh 1 min |
| 30 | Save 按钮灰色 | 名字输进了图表搜索框 | 标题框在画布顶部 |
| 31 | 切换数据集配置全丢 | Superset Swap dataset 会重置配置 | 切换前记录配置 |
| 32 | removeChild 报错 | 浏览器翻译插件改 DOM | 硬刷新 + 关翻译 |
| 33 | ERROR 1064 | `current_time` 是保留字 | 别名换 `now_time` |

> **最有价值的一条**：坑 24（配置文件未加载）。它让前面所有时区与缓存修改全部无效，
> 排查时却一直以为是配置内容写错了。**改配置前先确认配置有没有被加载**，
> 判定标准就是启动日志里那行 `Loaded your LOCAL configuration at [...]`。
>
> **第二有价值的**：坑 29（图表是快照）。这不是 Bug 而是所有 BI 工具的通性，
> 实时数仓的价值在于**后端链路延迟低**，前端的"动"靠仪表盘轮询实现。

---

## 五、项目结构

```
taobao-realtime-dw/
├── order_producer.py      # 高仿真订单生产者（6 大仿真参数，UTC 时间戳）
├── requirements.txt       # Python 依赖
├── sql/
│   ├── flink_realtime.sql # Flink SQL 脚本（源表 + 双 Sink + TUMBLE 聚合）
│   ├── mysql_schema.sql   # MySQL 建表 + 可视化 VIEW（v_dws_category_sales）
│   └── kafka_topics.sh    # Kafka 主题创建命令
├── .gitignore
└── README.md              # 本文档（含 33 条踩坑记录）
```

---

## 六、运行效果

### 实时销售大屏（淘宝实时销售看板）

四个图表 + 1 分钟自动轮询，打开后无需任何手动操作即可看到数据自己往前走：

- **数据最新时间**（Big Number）：显示 `MAX(window_end)`，每分钟前进一格，是数据新鲜度的直接证据
- **累计销售额**（Big Number with Trendline）：大数字持续增长，底部迷你趋势线同步延长
- **品类销售趋势**（Line Chart）：8 个品类分钟级折线，最右端每分钟新增一个数据点
- **品类销售额对比**（Table Chart）：`category` / `window_end` / `SUM(total_sales)` / `SUM(order_count)` 明细

### 实时性实测

某一时刻的三方对齐验证（北京时间 15:44）：

| 观测点 | 数值 |
|--------|------|
| MySQL `MAX(window_end)` | `2026-09-03 06:44:00` (UTC) |
| MySQL `NOW()` | `2026-09-03 14:44:24` (北京时间) |
| Superset `Last queried at` | `09/03/2026 2:44:54 PM` |
| Superset 折线图最右端 tooltip | `Thu Sep 03, 02:44 PM` |
| Results 行数 | 506 行 ÷ 7 品类 ≈ 72 分钟，覆盖 `13:32 → 14:44` |

四个时间点完全对齐，**端到端延迟 ≤ 1 分钟**（即 Flink TUMBLE 窗口的固有延迟）。

### 数据样例

```
+-------------+---------------------+-------------+-------------+
| category    | window_end          | total_sales | order_count |
+-------------+---------------------+-------------+-------------+
| Electronics | 2026-09-03 06:44:00 |    21903.44 |          11 |
| Home        | 2026-09-03 06:44:00 |     1391.20 |           3 |
| Clothing    | 2026-09-03 06:44:00 |     7462.18 |          19 |
+-------------+---------------------+-------------+-------------+
```

> `window_end` 存的是 UTC 值，Superset 按 `DISPLAY_TIMEZONE = 'Asia/Shanghai'` 渲染成北京时间 `14:44`。

---

## 七、后续优化方向

1. **更多指标**：客单价、转化率、UV/PV、复购率
2. **实时告警**：某品类销售额突降 → 钉钉/邮件通知
3. **OLAP 升级**：MySQL → Doris/ClickHouse（生产级查询性能）
4. **维度扩展**：按城市、年龄段、支付方式多维分析
5. **CEP 复杂事件处理**：检测异常订单模式（刷单、欺诈）
6. **结果表生命周期管理**：按天分区 + TTL 定期清理，避免 DWS 表无限增长；或改用滚动时间窗口只查最近 1 小时（前提是解决坑 27 的过滤器时区基准问题）
7. **时区基准统一**：把 `DATETIME` 换成 `TIMESTAMP`（MySQL 会带时区语义），或让 VIEW 输出北京时间 + `DISPLAY_TIMEZONE='UTC'`，使相对时间过滤器可用
8. **秒级延迟**：TUMBLE 1 分钟窗口改为 10 秒滑动窗口，或引入 Flink CDC 直连；同时把 BI 数据源换成 Doris/ClickHouse 承接高并发轮询
9. **数据质量监控**：对 1% 脏数据的拦截量、Kafka 消费 Lag、Flink Checkpoint 失败次数做告警

---

## 八、关联项目

- 离线数仓：[taobao-dw-project](https://github.com/YangLv-Analyst/taobao-dw-project)
- 实时数仓：[taobao-realtime-dw](https://github.com/YangLv-Analyst/taobao-realtime-dw)（本仓库）
