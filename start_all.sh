#!/bin/bash
# ============================================================
# start_all.sh -- 一键拉起淘宝实时数仓链路
#
#   Kafka -> order_producer -> Flink(2 slot) -> 双 Sink -> MySQL
#
# 铁律：所有守护进程必须用 setsid 脱离会话。WSL 会话结束时会向整个进程组
#       发 SIGHUP，JobManager 日志里会留下 "RECEIVED SIGNAL 1: SIGHUP.
#       Shutting down as requested."，集群转身就没了（README 坑 34）。
#       Windows 侧的 Start-Process 同理会被 Job Object 连带回收。
#
# 幂等：已在运行的组件自动跳过，可反复执行。
# 用法：bash start_all.sh
# 日志：~/rt-logs/{kafka,flink-cluster,tm*,sqlclient,producer}.log
# ============================================================
set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

FLINK_HOME="${FLINK_HOME:-/opt/flink}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
JAVA17="${JAVA17:-/usr/lib/jvm/java-17-openjdk-amd64}"
REST_PORT="${REST_PORT:-8090}"
BOOTSTRAP="${BOOTSTRAP:-127.0.0.1:9092}"
PYLIBS="${PYLIBS:-$HOME/pylibs}"
LOG_DIR="${LOG_DIR:-$HOME/rt-logs}"
NEED_SLOTS="${NEED_SLOTS:-2}"
NEED_JOBS="${NEED_JOBS:-2}"
MIN_AVAIL_MB="${MIN_AVAIL_MB:-1500}"
TOPICS="${TOPICS:-taobao_orders taobao_dws_category_sales}"

mkdir -p "$LOG_DIR"

log()  { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
die()  { printf '[%s] FATAL: %s\n' "$(date '+%H:%M:%S')" "$*" >&2; exit 1; }
have() { pgrep -f "$1" > /dev/null 2>&1; }

flink_slots() {
  curl -s "http://localhost:${REST_PORT}/overview" 2>/dev/null |
    python3 -c 'import sys,json;print(json.load(sys.stdin).get("slots-total",0))' 2>/dev/null
}
flink_running_jobs() {
  curl -s "http://localhost:${REST_PORT}/jobs/overview" 2>/dev/null |
    python3 -c 'import sys,json;print(sum(1 for j in json.load(sys.stdin).get("jobs",[]) if j.get("state")=="RUNNING"))' 2>/dev/null
}

# ------------------------------------------------------------ step 0  内存预检
AVAIL_MB="$(free -m | awk '/^Mem:/{print $7}')"
AVAIL_MB="${AVAIL_MB:-0}"
log "step0 memory available = ${AVAIL_MB}MB (threshold ${MIN_AVAIL_MB}MB)"
if [ "$AVAIL_MB" -lt "$MIN_AVAIL_MB" ]; then
  log "内存不足，TaskManager 随时会被饿死并触发心跳超时，而作业是"
  log "NoRestartBackoffTimeStrategy，失败后不会自愈（README 坑 35）。请先释放："
  log "  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64   # Hadoop/Spark 用 Java 8"
  log "  /opt/spark/sbin/stop-all.sh"
  log "  /opt/hadoop/sbin/stop-yarn.sh && /opt/hadoop/sbin/stop-dfs.sh"
  log "  pkill -f proc_metastore"
  exit 1
fi

export JAVA_HOME="$JAVA17"
export PATH="$JAVA_HOME/bin:$PATH"

# ------------------------------------------------------------ step 1  Kafka
if have 'kafka\.Kafka'; then
  log "step1 Kafka already running, skip"
else
  KAFKA_CONF=""
  for c in "$KAFKA_HOME/config/server.properties" "$KAFKA_HOME/config/kraft/server.properties"; do
    [ -f "$c" ] && { KAFKA_CONF="$c"; break; }
  done
  [ -n "$KAFKA_CONF" ] || die "找不到 Kafka 配置文件，请设 KAFKA_HOME"
  log "step1 starting Kafka ($KAFKA_CONF)"
  setsid "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONF" \
    < /dev/null > "$LOG_DIR/kafka.log" 2>&1 &
  i=0
  while [ "$i" -lt 60 ]; do have 'kafka\.Kafka' && break; sleep 1; i=$((i+1)); done
  have 'kafka\.Kafka' || die "Kafka 60 秒内未起来，见 $LOG_DIR/kafka.log"
  log "step1 Kafka up (${i}s)"
fi

# ------------------------------------------------------------ step 1b  主题
EXISTING="$("$KAFKA_HOME/bin/kafka-topics.sh" --list --bootstrap-server "$BOOTSTRAP" 2>/dev/null)"
for T in $TOPICS; do
  if printf '%s\n' "$EXISTING" | grep -qx "$T"; then
    log "step1b topic $T exists"
  else
    log "step1b creating topic $T"
    "$KAFKA_HOME/bin/kafka-topics.sh" --create --topic "$T" --bootstrap-server "$BOOTSTRAP" \
      --partitions 1 --replication-factor 1 > /dev/null 2>&1 ||
      die "创建主题 $T 失败"
  fi
done

# ------------------------------------------------------------ step 2  Flink 集群
SLOTS="$(flink_slots)"; SLOTS="${SLOTS:-0}"
if [ "$SLOTS" -ge "$NEED_SLOTS" ]; then
  log "step2 Flink already has $SLOTS slots, skip"
else
  log "step2 (re)starting Flink cluster (slots=$SLOTS, need=$NEED_SLOTS)"
  "$FLINK_HOME/bin/stop-cluster.sh" < /dev/null > /dev/null 2>&1
  sleep 3
  setsid "$FLINK_HOME/bin/start-cluster.sh" < /dev/null > "$LOG_DIR/flink-cluster.log" 2>&1
  sleep 8
  # config.yaml 默认 numberOfTaskSlots: 1，两条 INSERT 需要 2 个 slot，必须补启 TM。
  # 注意配置只在集群启动时读一次，改完 config.yaml 必须 stop/start-cluster.sh。
  n=0
  while [ "$n" -lt 3 ]; do
    S="$(flink_slots)"; S="${S:-0}"
    [ "$S" -ge "$NEED_SLOTS" ] && break
    n=$((n+1))
    log "step2 starting extra TaskManager #$n (slots=$S)"
    setsid "$FLINK_HOME/bin/taskmanager.sh" start < /dev/null > "$LOG_DIR/tm$n.log" 2>&1
    sleep 8
  done
  SLOTS="$(flink_slots)"; SLOTS="${SLOTS:-0}"
  [ "$SLOTS" -ge "$NEED_SLOTS" ] ||
    die "Flink slot 只有 $SLOTS，需要 $NEED_SLOTS。见 $LOG_DIR/flink-cluster.log"
  log "step2 Flink ready, slots=$SLOTS"
fi

# ------------------------------------------------------------ step 3  生产者
# 必须先清空再启一个：残留多个实例会让销售额凭空翻倍（README 坑 38）
log "step3 clearing all existing producers"
pkill -f 'order_producer\.py' 2>/dev/null
sleep 2
[ -f "$REPO/order_producer.py" ] || die "找不到 $REPO/order_producer.py"
[ -d "$PYLIBS/kafka" ] || die "缺少 kafka-python。安装：
  $HOME/superset-env/bin/python -m pip install --target $PYLIBS kafka-python"
log "step3 starting ONE producer from $REPO/order_producer.py"
PYTHONPATH="$PYLIBS" setsid nohup python3 -u "$REPO/order_producer.py" \
  > "$LOG_DIR/producer.log" 2>&1 < /dev/null &
sleep 6
N="$(pgrep -fc 'order_producer\.py' 2>/dev/null)"; N="${N:-0}"
[ "$N" -eq 1 ] || die "生产者实例数 = $N，应为 1。见 $LOG_DIR/producer.log"
log "step3 producer running (instances=$N, pid=$(pgrep -f 'order_producer\.py'))"

# ------------------------------------------------------------ step 4  提交作业
RUNNING="$(flink_running_jobs)"; RUNNING="${RUNNING:-0}"
if [ "$RUNNING" -ge "$NEED_JOBS" ]; then
  log "step4 already $RUNNING jobs RUNNING, skip"
else
  [ -f "$REPO/sql/flink_realtime.sql" ] || die "找不到 $REPO/sql/flink_realtime.sql"
  log "step4 submitting jobs via sql-client -f (detached)"
  setsid "$FLINK_HOME/bin/sql-client.sh" -f "$REPO/sql/flink_realtime.sql" \
    < /dev/null > "$LOG_DIR/sqlclient.log" 2>&1 &
  i=0
  while [ "$i" -lt 30 ]; do
    sleep 3; i=$((i+1))
    RUNNING="$(flink_running_jobs)"; RUNNING="${RUNNING:-0}"
    [ "$RUNNING" -ge "$NEED_JOBS" ] && break
  done
  [ "$RUNNING" -ge "$NEED_JOBS" ] ||
    die "作业未 RUNNING（当前 $RUNNING / $NEED_JOBS）。见 $LOG_DIR/sqlclient.log
  提示：sql-client 卡死超时往往不是 SQL 写错了，而是 JM 已被 SIGHUP 杀掉（坑 34）"
  log "step4 $RUNNING jobs RUNNING"
fi

# ------------------------------------------------------------ step 5  巡检
# 用 --fast：完整巡检要等 75 秒做第二次采样，启动时没必要卡这么久
log "step5 running health_check.sh --fast"
bash "$REPO/health_check.sh" --fast
log "完整巡检（含 75 秒双采样）请单独执行: bash $REPO/health_check.sh"
