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

# 展示时区。⚠️ 它对折线图 X 轴【无效】——DATETIME 是 naive 值，前端不做本地化（见坑 20 与 6.5）
# 真正的 +8 必须写在 MySQL 的 VIEW 里；此项保留只为统一 SQL Lab / 日志等带时区场景的口径
DISPLAY_TIMEZONE = 'Asia/Shanghai'

# 实时数仓必须禁用全部 4 类缓存通道，缺一个都会导致图表显示旧数据
CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}
DATA_CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}
FILTER_STATE_CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}
EXPLORE_FORM_CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}

# ⚠️ 必加。修复 Superset 6.1.0 + sqlglot 28.10.1 破坏 MySQL 时间粒度表达式的 Bug：
# sqlglot 会删掉 DATE_ADD 第一个参数上的 DATE()，使分钟粒度变成
# 「把当天的时分再加到自己身上」，折线图因此画到未来（实际 13:14 画成次日 03:14）。
# 原理、替代表达式的逐条验证与端到端实测见 6.9，坑 36。
# 改完必须重启 Superset（该配置只在 app 初始化时读一次）。
TIME_GRAIN_ADDON_EXPRESSIONS = {
    'mysql': {
        'PT1S': '{col}',
        'PT1M': 'TIMESTAMP(DATE({col}), MAKETIME(HOUR({col}), MINUTE({col}), 0))',
        'PT1H': 'TIMESTAMP(DATE({col}), MAKETIME(HOUR({col}), 0, 0))',
        'P1D':  'DATE({col})',
        'P1W':  'DATE({col} - INTERVAL (DAYOFWEEK({col}) - 1) DAY)',
        '1969-12-29T00:00:00Z/P1W':
            'DATE({col} - INTERVAL (DAYOFWEEK({col} - INTERVAL 1 DAY) - 1) DAY)',
        'P1M':  'DATE({col} - INTERVAL (DAYOFMONTH({col}) - 1) DAY)',
        'P3M':  'MAKEDATE(YEAR({col}), 1) + INTERVAL QUARTER({col}) QUARTER - INTERVAL 1 QUARTER',
        'P1Y':  'DATE({col} - INTERVAL (DAYOFYEAR({col}) - 1) DAY)',
    },
}
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
- Filters 一律用**滚动窗口** `DATEADD(DATETIME("now"), -1, hour) : now`，让最右端永远贴着当前分钟（见坑 27）
- Row limit 一律 **50000**（见坑 26）
- Contribution Mode 一律 **None**（见坑 28）
- X 轴 Time Format 保持默认 `smart_date`，**不要**加 `local!` 前缀（会与 VIEW 的 +8 叠成 +16，见坑 20）
- Big Number 显示日期若变成 Unix 时间戳或 `NaN`，改用 Table 类型渲染

> **坑 17：折线图只有两行**
> - 现象：折线图只显示 2 个数据点
> - 原因：Time Grain 默认是 `Day`，按天聚合
> - 解决：左侧 Time 区域 → Time Grain 改为 `Minute`

> **坑 18：category 误放 Metrics**
> - 现象：弹窗要求选聚合方式
> - 原因：category 是维度，不应放 Metrics 区域
> - 解决：category 拖到 Group By/Dimensions，不选聚合

#### 6.5 时区攻坚实录（最终定案）

这是整个项目耗时最长、也最容易反复的一段排查。核心矛盾是：
**MySQL 的 `DATETIME` 类型不携带任何时区元数据**，所以同一个 `2026-09-03 05:16:00`，
生产者、Flink、MySQL、Superset 四方各有各的解释。

**最终定案：存储层统一 UTC，+8 只做在 VIEW 里，且全链路只做这一次。**

| 层级 | 配置 | 结果 |
|------|------|------|
| Python 生产者 | `datetime.utcnow().strftime(...)` | 生成 UTC 字符串 |
| Flink JVM | `env.java.opts.all: -Duser.timezone=UTC` | 窗口时间按 UTC 计算 |
| JDBC Sink | URL 加 `serverTimezone=UTC` | 写入不被驱动二次转换 |
| MySQL 物理表 | `DATETIME` 存 UTC 原值 | `04:58` |
| **VIEW** | **`DATE_ADD(window_end, INTERVAL 8 HOUR)`** | **`12:58`** ✅ |
| Superset 前端 | X 轴 Time Format = `smart_date`（默认，不加前缀） | 原样渲染成 `12:58 PM` ✅ |

##### 为什么 +8 必须写在 VIEW，而不是靠 `DISPLAY_TIMEZONE`

这是 2026-09-04 直接读 Superset 前端 bundle 取证得到的结论。`smart_date` 的注册代码是这样的：

```js
let i = "smart_date";
function n(e) {
  return (0, a.A)({
    id: i,
    label: "Adaptive Formatting",
    formats: {
      millisecond: ".%Lms", second: ":%Ss", minute: "%I:%M", hour: "%I %p",
      day: "%a %d", week: "%b %d", month: "%B", year: "%Y"
    },
    locale: e
  });
}
```

关键在于它**没有传 `useLocalTime`** → 默认 `false` → 格式化时走
`getUTCFullYear / getUTCHours / getUTCMinutes ...` 分支 → **把库里的 naive 值原样渲染，不做任何本地化**。

由此推出三条硬结论：

1. `DISPLAY_TIMEZONE` 与 JDBC URI 上的时区参数，只对「已带时区信息的值」做换算；
   对 naive `DATETIME` 的图表时间轴**完全无效**——这解释了为什么改了几十次配置都毫无反应。
2. 轴标签里出现的 `08 AM / 12 PM / 04 PM / 08 PM / 11 PM`，正是 `formats.hour = "%I %p"`
   在渲染 naive 值，不是时区转换的产物。
3. bundle 里确实存在 `local!` 前缀（`let a = "local!"`），但 `smart_date` 是**已注册的 key**，
   命中 `this.has(t)` 会直接返回、根本不解析前缀；即便解析成功，也会与 VIEW 的 +8 叠成 +16。
   **所以这个前缀不要加。**

##### 排查过程中走过的弯路

| 尝试方案 | 现象 | 结论 |
|---------|------|------|
| VIEW `DATE_ADD(+8)` + `DISPLAY_TIMEZONE` | 显示时间随配置组合漂移 4~16 小时 | ⚠️ 现象真实但归因错了：漂移来自「链路中途停摆 + 图表无时间过滤器 + Row limit 截断」三者叠加，并非双重转换 |
| VIEW `DATE_SUB(-8)` | 显示成前一天晚上 | ❌ 方向反了 |
| 手动 `UPDATE` 历史数据 +8 | MAX 时间比当前还快 8h | ❌ 污染数据。时区是展示层问题，永远不要动存储层的值 |
| VIEW 直通 + `DISPLAY_TIMEZONE` | tooltip 慢 8 小时，调配置无反应 | ❌ `DISPLAY_TIMEZONE` 对 naive DATETIME 无效 |
| **VIEW `+8` + `smart_date`（不加前缀）** | **tooltip = 当前北京时间，`lag_sec` 6~51 秒** | ✅ **正解** |

> **本节最大的教训**：前几轮之所以反复失败，是因为一直在用「改配置 → 看截图」的黑盒方式试错，
> 而那个配置项对 naive `DATETIME` 根本不生效，怎么改都不会有反应。
> 真正的破局点是**读前端 bundle 的注册代码** + **抓服务器实际执行的 SQL**，用代码级证据代替猜测。

> **坑 19：全链路时区不一致**
> - 现象：Superset tooltip 时间比北京时间慢 8 小时
> - 原因：生产者用 `datetime.now()` 生成北京时间字符串，Flink JVM 却按 UTC 解释
> - 解决：生产者改 `datetime.utcnow()`，Flink JVM 加 `-Duser.timezone=UTC`，JDBC URL 加 `serverTimezone=UTC`

> **坑 20：以为 `DISPLAY_TIMEZONE` 会自动 +8（本项目的头号认知陷阱）**
> - 现象：tooltip 时间比北京时间慢 8 小时；反复调 `DISPLAY_TIMEZONE`、JDBC URI 时区参数都毫无反应
> - 原因：MySQL `DATETIME` 是 naive 值，而折线图 X 轴的 `smart_date` 在前端注册时**不传 `useLocalTime`**
>   （默认 false），格式化走 `getUTC*` 分支，把 naive 值原样渲染。`DISPLAY_TIMEZONE` 只作用于
>   「已带时区信息的值」，对图表时间轴无效
> - 解决：**把 +8 写在 VIEW 里**（`DATE_ADD(window_end, INTERVAL 8 HOUR)`），X 轴保持默认 `smart_date`、
>   不加 `local!` 前缀。全链路有且只有一次 +8
> - ⚠️ 本条曾长期被误记为「VIEW 与 DISPLAY_TIMEZONE 双重转换，所以 VIEW 必须直通」，
>   该结论已被 2026-09-04 的 bundle 取证推翻，完整证据见 6.5

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

> **坑 27：相对时间过滤器查不到数据（根因是 VIEW 直通，不是过滤器有 Bug）**
> - 现象：VIEW 直通时设 `1 hour ago → now`，图表 No data，生成的条件是 `window_end >= 2026-09-03T13:23:48`
> - 原因：Superset 用**服务器本地时间**（WSL 继承 Windows = 北京时间）计算相对范围，
>   而直通 VIEW 输出的是 UTC 值，两边差 8 小时，条件永远匹配不上
> - 解决：VIEW `+8` 之后两边口径天然一致，滚动窗口直接可用。实测（2026-09-04 12:46）
>   `DATEADD(DATETIME("now"), -1, hour) : now` 编译出的真实条件是
>   `window_end >= STR_TO_DATE('2026-09-04 11:46:06.000000','%Y-%m-%d %H:%i:%s.%f')`
>   `AND window_end < STR_TO_DATE('2026-09-04 12:46:06.000000',...)`，`error = None`，返回 50 行 ✅
> - 各时间范围表达式的实测语义（很容易选错）：
>   - `Last hour` / `Last 30 minutes` → **直接报错** `From date cannot be larger than to date`，不可用
>   - `Last day` → 昨天 00:00 ~ 今天 00:00，**不含今天**
>   - `today` → until = 今天 00:00，同样看不到当前数据
>   - `No filter` → 不生成 WHERE，返回全表历史（几千行密集成团，且没有"实时"观感）
>   - `DATEADD(DATETIME("now"), -1, hour) : now` → **唯一能表达"最近 1 小时"的写法**

> **坑 28：Contribution Mode 被设成 Row**
> - 现象：Y 轴变成 30% / 40% / 50%，看不到真实销售额，折线挤成一团
> - 原因：Contribution Mode = Row 会按行归一化成百分比
> - 解决：改成 **None**

**实时性三重校验法**（排查时靠这三条定位问题，比反复改配置有效得多）：

```bash
# 校验 1：底层数据是否实时（latest_utc + 8h 应约等于 beijing_now）
mysql -u flink -pflink123 taobao_realtime -e \
  "SELECT MAX(window_end) AS latest_utc, NOW() AS beijing_now, COUNT(*) AS total FROM dws_category_sales;"

# 校验 2：VIEW 是否正确 +8（max_bj 应等于 max_utc + 8 小时，且约等于 NOW()）
# lag_sec 稳定在 0~60 秒 = 链路健康；持续增大 = Flink 作业或生产者已停摆（见坑 34/35）
mysql -u flink -pflink123 taobao_realtime -e \
  "SELECT MAX(window_end) AS max_utc, NOW() AS now_bj FROM dws_category_sales;
   SELECT MAX(window_end) AS max_bj,  NOW() AS now_bj FROM v_dws_category_sales;
   SELECT TIMESTAMPDIFF(SECOND, MAX(window_end), NOW()) AS lag_sec FROM v_dws_category_sales;"

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

#### 6.8 稳定性攻坚实录（链路"突然不动了"）

时区与滚动窗口都配好之后还剩最后一类问题：**数据自己停了**。
现象是 MySQL 的 `MAX(window_end)` 长时间不变、折线最右端不再前进，
但 Superset 一切正常、配置也没动过。这类故障有三个互相独立的成因，必须逐个排除。

> **坑 34：守护进程被 SIGHUP / Job Object 回收（最隐蔽）**
> - 现象：用 `wsl -e bash script.sh` 启动的 Flink 集群，脚本一结束就全没了；JobManager 日志里
>   明明白白写着 `RECEIVED SIGNAL 1: SIGHUP. Shutting down as requested.`
>   Windows 侧用 `Start-Process -WindowStyle Hidden` 启的 Python 生产者同理，会无声消失，
>   stderr 里只剩一行 DeprecationWarning
> - 原因：WSL 会话结束时向整个进程组发 SIGHUP；Windows 侧自动化终端退出时，
>   Job Object 会连带回收整棵子进程树
> - 解决：一律用 `setsid` 脱离会话，并把三个标准流全部接管
>   ```bash
>   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
>   export PATH=$JAVA_HOME/bin:$PATH
>   setsid /opt/flink/bin/start-cluster.sh     < /dev/null > /tmp/fc.log   2>&1
>   setsid /opt/flink/bin/taskmanager.sh start < /dev/null > /tmp/tm2.log  2>&1
>   setsid /opt/flink/bin/sql-client.sh -f ~/flink_realtime.sql \
>          < /dev/null > /tmp/sqlclient.log 2>&1 &
>   PYTHONPATH=~/pylibs setsid nohup python3 -u ~/order_producer.py \
>          > ~/producer.log 2>&1 < /dev/null &
>   ```
> - 验证：**新开一个会话**执行 `ps -eo pid,cmd | grep -E 'TaskManagerRunner|order_producer'`，仍能查到才算成功
> - 附带一条：`sql-client.sh -f` 卡死超时（exit 124）往往不是 SQL 写错了，而是 JM 已经被 SIGHUP 杀了

> **坑 35：内存耗尽连带杀死 TaskManager，作业 FAILED 且不自愈**
> - 现象：`curl localhost:8090/overview` 返回 `taskmanagers: 0, slots-total: 0`；
>   `jps` 里只剩 JobManager，TaskManagerRunner 消失；作业状态 FAILED
> - 原因：单节点同时跑 Hadoop(NameNode+DN+SNN+RM+NM) + Spark(Master+Worker) + Hive metastore
>   + Flink + Kafka + Superset，7.6G 内存用到 available 只剩 690M、2G swap 全部占满。
>   TaskManager 被拖死后心跳超时：`TimeoutException: Heartbeat of TaskManager with id ... timed out`，
>   而作业用的是 `NoRestartBackoffTimeStrategy`，**失败后不会自动重启**
> - 解决：跑实时链路时先停掉离线集群，可释放约 4GB
>   ```bash
>   export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64   # Hadoop/Spark 用 Java 8
>   /opt/spark/sbin/stop-all.sh
>   /opt/hadoop/sbin/stop-yarn.sh && /opt/hadoop/sbin/stop-dfs.sh
>   pkill -f proc_metastore
>   ```
> - 另外注意 `config.yaml` 里 `numberOfTaskSlots: 1`，两个 INSERT 作业需要 2 个 slot，
>   必须额外 `taskmanager.sh start` 启第二个 TM；且配置只在集群启动时读一次，改完必须 stop/start-cluster.sh

> **坑 36：sqlglot 在【执行路径】上重渲染 SQL，删掉了时间粒度表达式里的 `DATE()`（本项目最深的坑）**
> - 现象：折线图 tooltip 显示**未来时间**（实际 `Sep 04 13:40`，画成 `Sat Sep 05, 03:14 AM`），
>   X 轴标签跟着变成 `03 AM`；相邻数据点间隔从 1 分钟变成 **2 分钟**；
>   而 MySQL 里连那个日期的行都不存在（`window_end > NOW()` 是空集）
> - 面板里的粒度表达式是
>   `DATE_ADD(window_end, INTERVAL (HOUR(window_end)*60 + MINUTE(window_end)) MINUTE)`，**少了 `DATE()` 包裹**
> - ⚠️ 我最初判定这只是 `sqlglot.transpile(..., pretty=True)` 的**展示层美化失真**、
>   服务器执行的是带 `DATE()` 的正确版本，并据此写下"这个表达式是恒等变换、不可能造成偏移"。
>   **前半句对（面板输出确实与 sqlglot 逐字符一致），后半句错得离谱**——
>   sqlglot 同样出现在执行路径上，被改写后的 SQL 才是真正送进 MySQL 的那一条
> - 真实调用链（逐层抓出来的，不是推测）：
>   ```
>   superset/models/core.py:769   get_df
>    → core.py:688                _execute_sql_with_mutation_and_logging
>        script = SQLScript(sql, self.db_engine_spec.engine)
>    → superset/sql/parse.py:1290  SQLScript.__init__ → split_script
>    → parse.py:578                sqlglot.parse(script, dialect='mysql')
>    → 回到 core.py                statement.format()   # 把 AST 渲染回字符串再执行
>   ```
> - sqlglot 28.10.1 认为 `DATE_ADD`/`DATE_SUB` **第一个参数**上的 `DATE()` 冗余，直接删掉：
>   `DATE_ADD(x, INTERVAL (HOUR(x)*60+MINUTE(x)) MINUTE)` = 把当天的时分再加到自己身上，
>   `13:07:42 → 2026-09-05 02:14:42`。分钟间距也因此从 1 分钟变 2 分钟
>   （`f(13:00)=02:00`、`f(13:01)=02:02`），**这是识别本坑的指纹**
> - 影响面：MySQL 的 `SECOND/MINUTE/HOUR/WEEK/MONTH/QUARTER/YEAR` 粒度**全部中招，只有 DAY 幸免**
> - 解决：用官方配置项 `TIME_GRAIN_ADDON_EXPRESSIONS` 覆盖内置模板，完整方案与验证见 **6.9**
> - 教训：**判断"某段代码有没有被执行"，不要看输出像不像，要看结果集对不对。**
>   定案靠的是把两边结果集对撞——同一条 SQL 直接丢给 MySQL 得到 60 个连续分钟，
>   Superset payload 里却是 60 个间隔 2 分钟的未来值。间隔变化排除了"平移"（时区），指向"二次变换"

> **坑 37：图表时间范围要在 4 个地方同步，改一处不生效**
> - 现象：改了 `params.time_range`，图表行为毫无变化
> - 原因：Superset 把同一份时间范围冗余存在 4 处，而查询时以 `query_context` 为准：
>   1. `params.time_range`
>   2. `params.adhoc_filters[operator == 'TEMPORAL_RANGE'].comparator`
>   3. `query_context.form_data` 里的同名两项
>   4. `query_context.queries[i].filters[op == 'TEMPORAL_RANGE'].val`
> - 解决：走 UI 改（Explore → Filters → 时间范围）会自动同步 4 处；
>   直接改 `superset.db` 的 `slices` 表则必须 4 处一起改。改完**无需重启服务**，浏览器硬刷新即生效

> **坑 38：多开了几个生产者，销售额凭空翻倍**
> - 现象：同一份数据被重复写入，销售额是预期的 2~3 倍
> - 原因：生产者反复"启动失败"后重试，实际留下了多个存活实例（Windows venv 一个、系统 Python 一个……）
> - 解决：启动前先按命令行特征清一遍，然后**只启一个**
>   ```bash
>   pkill -f order_producer.py                       # WSL 侧
>   ```
>   ```powershell
>   Get-CimInstance Win32_Process -Filter "Name like 'python%'" |
>     Where-Object { $_.CommandLine -like '*order_producer.py*' } |
>     ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
>   ```
>   再用 `kafka-get-offsets.sh` 观察 offset 是否稳定增长（该脚本需要 Java 17，否则报 `A JNI error`）

#### 6.9 时间粒度表达式被 sqlglot 破坏（本项目最深的一个坑）

时区（6.5）、滚动窗口（坑 27）、进程存活（6.8）全部搞定之后，折线图**还是**错的。
这一节推翻了坑 36 最初的判断，也是整个项目最后一个未解之谜。

##### 症状指纹（出现任意一条就该怀疑本坑）

| 症状 | 正常值 | 中招时 |
|------|--------|--------|
| 相邻数据点间隔 | 1 分钟 | **2 分钟** |
| tooltip 日期 | 当天 | **次日**，时间 = 当天时分再叠加一次 |
| MySQL `window_end > NOW()` | 空集 | 空集（**库里没有未来数据，是算出来的**） |
| `lag_sec` | 0~75 秒 | 0~75 秒（**链路完全健康，极易误判成前端问题**） |

后两行是关键：数据侧所有指标都是绿的，故障纯粹发生在「Superset 生成 SQL → 送进 MySQL」之间。

##### 根因：sqlglot 不只在展示层，它在执行路径上

调用链见坑 36。被改写前后的对照（对 `2026-09-04 13:07:42` 求值，MySQL 实测）：

| 表达式 | 结果 |
|--------|------|
| 内置模板（语义正确）`DATE_ADD(DATE(x), INTERVAL (HOUR(x)*60+MINUTE(x)) MINUTE)` | `2026-09-04 13:07:00` |
| **被删掉 `DATE()` 后（实际执行的）**`DATE_ADD(x, INTERVAL (HOUR(x)*60+MINUTE(x)) MINUTE)` | **`2026-09-05 02:14:42`** |

##### 修法：`TIME_GRAIN_ADDON_EXPRESSIONS`

`MySQLEngineSpec.get_time_grain_expressions()` 里有官方覆盖入口，不需要碰任何私有属性：

```python
time_grain_expressions = cls._time_grain_expressions.copy()
time_grain_expressions.update(
    app.config["TIME_GRAIN_ADDON_EXPRESSIONS"].get(cls.engine, {})
)
```

替换原则只有一条：**不要让 `DATE()` 出现在 `DATE_ADD` / `DATE_SUB` 的第一个参数上**。
配置全文在 **6.2**，逐条验证结果如下：

| 粒度 | 替代表达式 | sqlglot 往返 | MySQL 实测 |
|------|-----------|-------------|-----------|
| PT1S | `{col}` | 不变 | — |
| PT1M | `TIMESTAMP(DATE(x), MAKETIME(HOUR(x), MINUTE(x), 0))` | 不变 | `13:07:42 → 13:07:00` ✅ |
| PT1H | `TIMESTAMP(DATE(x), MAKETIME(HOUR(x), 0, 0))` | 不变 | `13:07:42 → 13:00:00` ✅ |
| P1D | `DATE(x)` | 不变 | ✅ |
| P1W | `DATE(x - INTERVAL (DAYOFWEEK(x)-1) DAY)` | 不变 | 周起 `2026-08-30` ✅ |
| P1W（周一起） | `DATE(x - INTERVAL (DAYOFWEEK(x - INTERVAL 1 DAY)-1) DAY)` | 只把 `1` 引号化 | ✅ |
| P1M | `DATE(x - INTERVAL (DAYOFMONTH(x)-1) DAY)` | 不变 | 月起 `2026-09-01` ✅ |
| P3M | `MAKEDATE(YEAR(x),1) + INTERVAL QUARTER(x) QUARTER - INTERVAL 1 QUARTER` | 只加括号 | 季度起 `2026-07-01` ✅ |
| P1Y | `DATE(x - INTERVAL (DAYOFYEAR(x)-1) DAY)` | 不变 | 年起 `2026-01-01` ✅ |

> `TIMESTAMP(DATE(x), ...)` 里的 `DATE()` 之所以安全：sqlglot 只对 `DATE_ADD`/`DATE_SUB`
> 的**第一个参数**做这个"化简"，`TIMESTAMP()` 的参数它不动。

改完**必须重启 Superset**（该配置只在 app 初始化时读一次），且照例用 `setsid` 启动（坑 34）。

##### 为什么不能靠 `SQL_QUERY_MUTATOR` 打补丁

执行顺序决定了它救不了：

```python
script = SQLScript(sql, self.db_engine_spec.engine)   # ← sqlglot 在这里就把 DATE() 删了
for i, statement in enumerate(script.statements):
    sql_ = self.mutate_sql_based_on_config(
        statement.format(),                           # ← mutator 拿到的是已被改写的 SQL
        is_split=True,
    )
```

默认 `MUTATE_AFTER_SPLIT = False` 时 `SQL_QUERY_MUTATOR` 甚至不会被调用；
即便调用，也已经在破坏之后。所以唯一干净的注入点就是粒度表达式本身。

##### 修复后的端到端实测（2026-09-04 14:22）

| 观测点 | 数值 |
|--------|------|
| 服务器执行的 SQL | `SELECT TIMESTAMP(DATE(window_end), MAKETIME(HOUR(window_end), MINUTE(window_end), 0)) AS window_end, ...` |
| WHERE | `window_end >= '2026-09-04 13:22:53' AND window_end < '2026-09-04 14:22:53'` |
| payload 的 x 值 | `13:23:00 → 14:22:00`，60 个 distinct 时间点 |
| 相邻间隔 | **`60.0` 秒**（修复前是 120 秒） |
| 超过当前时间的点数 | **0**（修复前 60 个全是未来值） |
| 被 sqlglot 破坏的粒度数 | **0 / 9** |
| `lag_sec` | 55 秒 |

##### 排查方法论（比结论更值钱）

定案靠的是**结果集对撞**，而不是读代码推断：

```
同一条 SQL 直接丢给 MySQL : min=2026-09-04 12:55  max=2026-09-04 13:54  60 个连续分钟
Superset payload 里的 x   : min=2026-09-05 01:58  max=2026-09-05 03:56  60 个点、间隔 2 分钟
```

间隔从 1 分钟变成 2 分钟，直接排除"整体平移"（时区问题）而指向"二次变换"；
再代入 `f(x) = x + (HOUR(x)*60 + MINUTE(x)) 分钟` 逐个吻合，证据链才闭合。

### 阶段 7：一键启停与健康巡检（日常入口）

前面阶段 1~6 的手工步骤只需在第一次搭建时走一遍。日常起停与排障用仓库根目录的两个脚本：

```bash
# 拉起整条链路（幂等，可反复执行；已在跑的组件自动跳过）
bash start_all.sh

# 只读巡检：进程 / slots / lag_sec / VIEW +8 / 图表时间范围一致性 / 时间粒度完整性
bash health_check.sh          # 两次采样，间隔 75 秒（必须 > 1 个窗口周期）
bash health_check.sh --fast   # 只采样一次，快速看一眼
```

`start_all.sh` 的执行顺序与判定标准：

| 步骤 | 动作 | 跳过条件 | 失败即退出 |
|------|------|---------|-----------|
| 0 | 内存预检 | — | available < 1500MB（坑 35） |
| 1 | 启动 Kafka（Java 17） | `kafka.Kafka` 已在跑 | 60 秒内起不来 |
| 1b | 确保两个主题存在 | 主题已在 `--list` 里 | 创建失败 |
| 2 | 启动 Flink 集群并补足 slot | REST `/overview` 的 `slots-total >= 2` | slot 仍不足 |
| 3 | 清掉全部生产者，只启一个 | — | 实例数 != 1（坑 38） |
| 4 | `sql-client.sh -f` 提交双 Sink | RUNNING 作业数已 >= 2 | 90 秒内未 RUNNING |
| 5 | 调用 `health_check.sh` | — | — |

所有守护进程一律 `setsid ... < /dev/null > log 2>&1 &`，日志落在 `~/rt-logs/`（坑 34）。

`health_check.sh` 有两条硬判定：

1. **`lag_sec` 必须落在 0~75 秒**。它是 `NOW() - MAX(v_dws_category_sales.window_end)`，
   比任何前端截图都可靠——图表只是一次查询的快照（坑 29）。
2. **时间粒度表达式必须能通过 sqlglot 往返**（第 `[6]` 节）。脚本会把配置里的 `PT1M`
   表达式先过一遍 sqlglot、再丢给 MySQL 对 `'2026-09-04 13:07:42'` 求值，结果必须是
   `2026-09-04 13:07:00`。这是坑 36 的回归门禁：**`lag_sec` 全绿但折线飞到未来**的那类故障，
   只有这一条能拦住。

健康时 `lag_sec` 会在 **0 ~ 约 65 秒之间来回振荡**：TUMBLE 窗口刚关闭时接近 0，
下一个窗口关闭前涨到 60 多。所以阈值必须留出一个空窗口的余量：

| `lag_sec` | 判定 | 处理 |
|-----------|------|------|
| ≤ 75 | HEALTHY | 无 |
| 76 ~ 180 | SLOW（警告，不计入失败） | 可能正撞上无成交的空分钟，隔两分钟再跑一次 |
| > 180 | STALLED（FAIL，退出码 1） | 按 **坑 34 → 坑 35 → 坑 38** 顺序排查 |

> ⚠️ **两次采样间隔固定 75 秒（`SAMPLE_GAP` 可覆盖），绝不能用 30 秒。**
> 窗口每分钟才吐一次结果，30 秒内有约一半概率跨不过窗口边界，会把健康链路
> 误报成「数据无增长」——这个坑写脚本时真踩了一次（13:21:23 与 13:21:53 两次采样
> `cnt` 都是 8890，脚本判 FAIL，而实际 `lag_sec` 只有 23→53 秒，链路完全正常）。
> 因此「增长判定」只作参考、不计入失败，判定标准只有 `lag_sec` 一条。

脚本退出码 0 = PASS、1 = FAIL，可直接挂到 crontab 或 CI 里做巡检。

---

## 四、踩坑总结（38 条）

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
| 20 | 时间慢 8 小时，调配置毫无反应 | `smart_date` 不传 `useLocalTime`，前端原样渲染 naive 值 | +8 写在 VIEW 里，只此一次 |
| 21 | MAX 时间比当前还快 | 手动 UPDATE 修时区污染数据 | TRUNCATE 让 Flink 重写 |
| 22 | Superset 启动即崩 | SQLite 不支持 server_side_cursors | 删掉 ENGINE_OPTIONS |
| 23 | Invalid decryption key | 改了 SECRET_KEY，旧密码解不开 | 重连 + sqlite3 绕过 ORM 清库 |
| 24 | **配置全部空转** | **未设 `SUPERSET_CONFIG_PATH`，配置文件没被加载** | **写进 `~/.bashrc` 永久生效** |
| 25 | 图表滞后 1.5 小时 | 只禁了 2 类缓存，还有 2 类在缓存 | 4 类缓存全设 NullCache |
| 26 | 2.4 小时后又变慢 | Row limit 1000 只够 142 分钟 | Row limit 改 50000 |
| 27 | 相对时间过滤器 No data | 过滤器用北京时间，直通 VIEW 输出 UTC | VIEW +8 后改用滚动窗口 |
| 28 | Y 轴变成百分比 | Contribution Mode = Row | 改成 None |
| 29 | 图表不会自己前进 | BI 图表是查询快照 | 仪表盘配 auto-refresh 1 min |
| 30 | Save 按钮灰色 | 名字输进了图表搜索框 | 标题框在画布顶部 |
| 31 | 切换数据集配置全丢 | Superset Swap dataset 会重置配置 | 切换前记录配置 |
| 32 | removeChild 报错 | 浏览器翻译插件改 DOM | 硬刷新 + 关翻译 |
| 33 | ERROR 1064 | `current_time` 是保留字 | 别名换 `now_time` |

### 稳定性层踩坑（坑 34~38）

| # | 坑 | 根因 | 解决 |
|---|-----|------|------|
| 34 | 集群/生产者一转身就没了 | WSL 会话结束发 SIGHUP；Windows Job Object 回收子进程树 | 一律 `setsid` + 接管三个标准流 |
| 35 | 作业 FAILED 且不自愈 | 内存耗尽饿死 TaskManager，心跳超时 + NoRestartBackoffTimeStrategy | 停掉离线集群释放 ~4GB，补启第二个 TM |
| 36 | **折线飞到未来、点距从 1 分钟变 2 分钟** | **sqlglot 在执行前重渲染 SQL，删掉粒度表达式里的 `DATE()`** | **`TIME_GRAIN_ADDON_EXPRESSIONS` 覆盖内置模板（6.9）** |
| 37 | 改了时间范围不生效 | 时间范围冗余存在 4 处，以 query_context 为准 | 4 处同步改，或直接走 UI |
| 38 | 销售额凭空翻倍 | 同时存活多个生产者实例 | 启动前按命令行特征清一遍，只留一个 |

> **最有价值的一条**：坑 24（配置文件未加载）。它让前面所有时区与缓存修改全部无效，
> 排查时却一直以为是配置内容写错了。**改配置前先确认配置有没有被加载**，
> 判定标准就是启动日志里那行 `Loaded your LOCAL configuration at [...]`。
>
> **第二有价值的**：坑 20（`DISPLAY_TIMEZONE` 对 naive DATETIME 无效）。
> 它决定了「改配置 → 看截图」这种试错方式注定失败——配置项根本不参与渲染，
> 怎么改都不会有反应。只有读前端 bundle、抓真实 SQL 才能定案。
>
> **第三有价值的**：坑 34（守护进程被回收）。它会让整条链路"看起来配好了却不动"，
> 而故障点在进程管理、不在任何配置里，排查方向极易跑偏到 Superset 身上。
>
> **第四有价值的**：坑 36（sqlglot 在执行路径上改写 SQL）。它是唯一一个
> 「数据侧全绿、`lag_sec` 正常、配置也全对」却仍然画错的故障，排查方向极易被带偏到时区或缓存上。
> 识别指纹是**相邻点距从 1 分钟变成 2 分钟**；修法是覆盖时间粒度表达式（6.9），
> 而不是去动 VIEW、时区或缓存。
>
> 另外，坑 29（图表是快照）不是 Bug 而是所有 BI 工具的通性，
> 实时数仓的价值在于**后端链路延迟低**，前端的"动"靠仪表盘轮询实现。

---

## 五、项目结构

```
taobao-realtime-dw/
├── order_producer.py      # 高仿真订单生产者（6 大仿真参数，UTC 时间戳）
├── start_all.sh           # 一键拉起链路（setsid 常驻 + 内存预检，幂等可反复执行）
├── health_check.sh        # 只读健康巡检（进程/slots/lag_sec/VIEW +8/图表配置/粒度完整性）
├── requirements.txt       # Python 依赖
├── sql/
│   ├── flink_realtime.sql # Flink SQL 脚本（源表 + 双 Sink + TUMBLE 聚合）
│   ├── mysql_schema.sql   # MySQL 建表 + 可视化 VIEW（v_dws_category_sales，含 +8）
│   └── kafka_topics.sh    # Kafka 主题创建命令
├── .gitignore
└── README.md              # 本文档（含 38 条踩坑记录）
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

### 实时性复验（2026-09-04，滚动窗口版）

链路稳定运行后的第二次取证，这次直接读服务器真正执行的 SQL 与过滤边界：

| 观测点 | 数值 |
|--------|------|
| 采样时刻（北京时间） | `2026-09-04 12:58:35` |
| VIEW `MAX(window_end)` | `2026-09-04 12:58:00` |
| **`lag_sec`（NOW − VIEW MAX）** | **6 秒** |
| 折线图滚动窗口范围 | `12:31 → 12:59`，129 行 / 24 个时间点 |
| Flink 集群 | `taskmanagers: 2, slots-total: 2`，2 个作业 RUNNING |
| 服务器实际 SQL 的 WHERE | `window_end >= STR_TO_DATE('2026-09-04 11:46:06.000000', ...)`<br>`AND window_end < STR_TO_DATE('2026-09-04 12:46:06.000000', ...)` |

> `lag_sec` 在 6~51 秒之间波动，这是 TUMBLE 1 分钟窗口的固有下限，符合"分钟级实时"目标。
>
> ⚠️ 滚动窗口刚启用时，如果链路是中途才恢复的，窗口左半段会有一段空洞
> （本例 `11:46 ~ 12:31` 无数据，最初只有右侧 20 多个点）。这是正常现象，
> 随时间推移会填满到 60 个时间点，**不是 Bug，不要因此又去改配置**。
>
> 另外正午时段 `hour_factor()` 系数只有 0.2（约 5 秒一单），部分品类在某些分钟没有成交，
> 折线天然是断续的，这恰恰是高仿真数据应有的形态。

### 实时性三验（2026-09-04 14:22，时间粒度修复后）

坑 36 修复之后的第三次取证，这次直接读服务器执行的 SQL 与 payload 里的 x 值（完整过程见 6.9）：

| 观测点 | 数值 |
|--------|------|
| 采样时刻（北京时间） | `2026-09-04 14:22:54` |
| VIEW `MAX(window_end)` | `2026-09-04 14:22:00` |
| `lag_sec` | 55 秒 |
| 服务器执行的粒度表达式 | `TIMESTAMP(DATE(window_end), MAKETIME(HOUR(window_end), MINUTE(window_end), 0))` |
| payload x 值范围 | `13:23:00 → 14:22:00`，60 个连续分钟，间隔严格 `60.0` 秒 |
| 未来时间点 | 0 个 |

至此「时区口径（坑 20）→ 滚动窗口（坑 27）→ 进程存活（坑 34/35）→ 粒度表达式（坑 36）」
四个环节全部闭环，折线图最右端贴着当前分钟。

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

> 物理表 `dws_category_sales.window_end` 存的是 UTC 值（`06:44`）；
> VIEW `v_dws_category_sales` 做 `DATE_ADD(..., INTERVAL 8 HOUR)` 输出北京时间（`14:44`）；
> Superset 前端的 `smart_date` 原样渲染这个 naive 值。**全链路有且只有 VIEW 这一次 +8。**

---

## 七、后续优化方向

1. **更多指标**：客单价、转化率、UV/PV、复购率
2. **实时告警**：某品类销售额突降 → 钉钉/邮件通知
3. **OLAP 升级**：MySQL → Doris/ClickHouse（生产级查询性能）
4. **维度扩展**：按城市、年龄段、支付方式多维分析
5. **CEP 复杂事件处理**：检测异常订单模式（刷单、欺诈）
6. **结果表生命周期管理**：按天分区 + TTL 定期清理。图表侧已改为滚动窗口只查最近 1 小时
   （坑 27 已解决），但物理表仍在无限增长，需要独立的归档策略
7. **时区语义下沉到存储层**：把 `DATETIME` 换成 `TIMESTAMP`（MySQL 会带时区语义），
   届时可去掉 VIEW 里的 `DATE_ADD(+8)`，改由 Superset 按 `DISPLAY_TIMEZONE` 正常换算。
   ⚠️ 改造时必须同步撤掉 VIEW 的 +8，否则叠加成 +16
8. ~~一键启停与健康巡检~~ **已完成**：`start_all.sh` / `health_check.sh` 已落地并通过实跑验证
   （幂等性、75 秒双采样、退出码门禁）。三类故障（TM 被杀、生产者被回收、内存耗尽）都会复发，日常靠它们兜底
9. **版本升级回归**：`TIME_GRAIN_ADDON_EXPRESSIONS`（6.9）是针对 Superset 6.1.0 + sqlglot 28.10.1
   的绕过方案，属于「用配置修正上游 Bug」。升级任一方后必须重跑 `bash health_check.sh`，
   其第 `[6]` 节会验证粒度表达式能否通过 sqlglot 往返；若上游已修复，可把这段配置整体删掉
10. **秒级延迟**：TUMBLE 1 分钟窗口改为 10 秒滑动窗口，或引入 Flink CDC 直连；同时把 BI 数据源换成 Doris/ClickHouse 承接高并发轮询
11. **数据质量监控**：对 1% 脏数据的拦截量、Kafka 消费 Lag、Flink Checkpoint 失败次数做告警

---

## 八、关联项目

- 离线数仓：[taobao-dw-project](https://github.com/YangLv-Analyst/taobao-dw-project)
- 实时数仓：[taobao-realtime-dw](https://github.com/YangLv-Analyst/taobao-realtime-dw)（本仓库）
