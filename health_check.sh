#!/bin/bash
# ============================================================
# health_check.sh -- 淘宝实时数仓健康巡检（只读，不改任何状态）
#
# 核心判定只有一条：
#   lag_sec = NOW() - MAX(v_dws_category_sales.window_end)
#
#   健康时 lag_sec 会在 0 ~ 约 65 秒之间【来回振荡】：TUMBLE 窗口刚关闭时接近 0，
#   下一个窗口关闭前涨到 60 多。所以阈值必须留出一个空窗口的余量：
#     <=  75 秒    健康
#     76 ~ 180 秒  警告（可能正撞上 hour_factor 低谷的空分钟，隔两分钟再跑一次）
#     >  180 秒    链路已停摆，按 坑34(进程被回收) -> 坑35(内存耗尽)
#                  -> 坑38(生产者多实例) 的顺序排查
#
# 为什么信 lag_sec 而不信截图：BI 图表是一次查询的快照（坑 29）。
#
# 但 lag_sec 只能证明「数据到得及时」，证明不了「图画得对」。所以还有第二条硬判定：
# 第 [6] 节检查时间粒度表达式能否通过 sqlglot 往返。Superset 6.1.0 + sqlglot 28.10.1
# 会在执行前删掉 DATE_ADD 第一个参数上的 DATE()，让分钟粒度变成「把当天的时分再加到
# 自己身上」，折线图因此画到未来，而 lag_sec 全程正常（坑 36，修法见 README 6.9）。
#
# 用法：bash health_check.sh           # 两次采样，间隔 75 秒（必须 > 1 个窗口周期）
#       bash health_check.sh --fast    # 只采样一次，快速看一眼
#
# ⚠️ 采样间隔绝不能用 30 秒：窗口每分钟才吐一次结果，30 秒内有约一半概率跨不过
#    窗口边界，会把健康链路误报成「数据无增长」。所以增长判定只作参考，
#    真正的判定标准是 lag_sec。
# ============================================================
set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REST_PORT="${REST_PORT:-8090}"
MYSQL_USER="${MYSQL_USER:-flink}"
MYSQL_PASS="${MYSQL_PASS:-flink123}"
MYSQL_DB="${MYSQL_DB:-taobao_realtime}"
SUPERSET_DB="${SUPERSET_DB:-$HOME/superset.db}"
WINDOW_MIN="${WINDOW_MIN:-60}"
SAMPLE_GAP="${SAMPLE_GAP:-75}"
FAST=0
[ "${1:-}" = "--fast" ] && FAST=1

FAILS=""
note() { printf '%s\n' "$*"; }
bad()  { FAILS="${FAILS}$1
"; }

q() { mysql -u "$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" -N -B -e "$1" 2>/dev/null; }

echo "============================================================"
echo " 实时链路健康巡检   $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "============================================================"

# ------------------------------------------------------------ 1 进程清单
echo ""
echo "--- [1] 进程清单 ---"
P_PROD="$(pgrep -fc 'order_producer\.py' 2>/dev/null)"; P_PROD="${P_PROD:-0}"
P_KAFKA="$(pgrep -fc 'kafka\.Kafka' 2>/dev/null)";        P_KAFKA="${P_KAFKA:-0}"
P_JM="$(pgrep -fc 'StandaloneSessionClusterEntrypoint' 2>/dev/null)"; P_JM="${P_JM:-0}"
P_TM="$(pgrep -fc 'TaskManagerRunner' 2>/dev/null)";      P_TM="${P_TM:-0}"
P_SUP="$(pgrep -fc 'superset run' 2>/dev/null)";          P_SUP="${P_SUP:-0}"
printf 'order_producer=%s  kafka=%s  flink_jm=%s  flink_tm=%s  superset=%s\n' \
  "$P_PROD" "$P_KAFKA" "$P_JM" "$P_TM" "$P_SUP"
[ "$P_PROD" -eq 1 ] || bad "生产者实例数=$P_PROD（应为 1）。0=被回收(坑34)，>1=销售额会翻倍(坑38)"
[ "$P_KAFKA" -ge 1 ] || bad "Kafka 未运行"
[ "$P_JM"   -ge 1 ] || bad "Flink JobManager 未运行(坑34)"
[ "$P_TM"   -ge 1 ] || bad "Flink TaskManager 未运行 —— 极可能是内存耗尽被饿死(坑35)"
[ "$P_SUP"  -ge 1 ] || bad "Superset 未运行"

# ------------------------------------------------------------ 2 Flink 集群
echo ""
echo "--- [2] Flink 集群 ---"
OV="$(curl -s "http://localhost:${REST_PORT}/overview" 2>/dev/null)"
if [ -z "$OV" ]; then
  echo "REST :$REST_PORT 无响应"
  bad "Flink REST 无响应，集群已挂(坑34)"
else
  echo "$OV" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("taskmanagers=%s slots-total=%s slots-available=%s jobs-running=%s jobs-failed=%s" % (
    d.get("taskmanagers"), d.get("slots-total"), d.get("slots-available"),
    d.get("jobs-running"), d.get("jobs-failed")))
' 2>/dev/null
  SLOTS="$(echo "$OV" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("slots-total",0))' 2>/dev/null)"
  JOBS="$(echo "$OV"  | python3 -c 'import sys,json;print(json.load(sys.stdin).get("jobs-running",0))' 2>/dev/null)"
  FAILED="$(echo "$OV" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("jobs-failed",0))' 2>/dev/null)"
  [ "${SLOTS:-0}" -ge 2 ] || bad "slots-total=$SLOTS，两条 INSERT 需要 2 个(坑35)"
  [ "${JOBS:-0}"  -ge 2 ] || bad "jobs-running=$JOBS，应为 2（kafka_dws_sink + mysql_dws_sink）"
  [ "${FAILED:-0}" -eq 0 ] || bad "有 $FAILED 个作业 FAILED。NoRestartBackoffTimeStrategy 不会自愈，必须重提(坑35)"
  echo "作业明细:"
  curl -s "http://localhost:${REST_PORT}/jobs/overview" 2>/dev/null | python3 -c '
import sys, json
for j in json.load(sys.stdin).get("jobs", []):
    print("  %-9s %s" % (j.get("state"), j.get("name")))
' 2>/dev/null
fi

# ------------------------------------------------------------ 3 数据新鲜度
echo ""
echo "--- [3] 数据新鲜度（核心判定） ---"
S0="$(q "SELECT CONCAT(NOW(),'|',COUNT(*),'|',IFNULL(MAX(window_end),'NULL')) FROM v_dws_category_sales;")"
L0="$(q "SELECT TIMESTAMPDIFF(SECOND, MAX(window_end), NOW()) FROM v_dws_category_sales;")"
echo "sample0  now/cnt/view_max = $S0"
echo "sample0  lag_sec = ${L0:-N/A}"
if [ "$FAST" -eq 0 ]; then
  echo "等待 ${SAMPLE_GAP} 秒做第二次采样（必须 > 1 个 TUMBLE 窗口周期，否则必然误报）..."
  sleep "$SAMPLE_GAP"
  S1="$(q "SELECT CONCAT(NOW(),'|',COUNT(*),'|',IFNULL(MAX(window_end),'NULL')) FROM v_dws_category_sales;")"
  L1="$(q "SELECT TIMESTAMPDIFF(SECOND, MAX(window_end), NOW()) FROM v_dws_category_sales;")"
  echo "sample1  now/cnt/view_max = $S1"
  echo "sample1  lag_sec = ${L1:-N/A}"
  C0="$(printf '%s' "$S0" | cut -d'|' -f2)"; C1="$(printf '%s' "$S1" | cut -d'|' -f2)"
  M0="$(printf '%s' "$S0" | cut -d'|' -f3)"; M1="$(printf '%s' "$S1" | cut -d'|' -f3)"
  if [ "${C1:-0}" -gt "${C0:-0}" ] || [ "$M1" != "$M0" ]; then
    echo "增长判定 = GROWING (cnt $C0 -> $C1, max $M0 -> $M1)"
  else
    # 仅提示、不计入失败：正午 hour_factor=0.2 时整分钟无成交是正常形态，
    # 链路是否停摆由下面的 lag_sec 说了算（生产者真死了 lag_sec 会一路涨过 180）
    echo "增长判定 = NO NEW ROW（仅供参考：该时段可能整分钟无成交，判定以 lag_sec 为准）"
  fi
  LAG="${L1:-}"
else
  LAG="${L0:-}"
fi
LAG="${LAG:-}"
case "$LAG" in
  ''|*[!0-9]*) echo "延迟判定 = UNKNOWN (查不到 lag_sec，VIEW 可能为空或 MySQL 连不上)"
               bad "无法计算 lag_sec：VIEW 无数据或 MySQL 连接失败" ;;
  *) if [ "$LAG" -le 75 ]; then
       echo "延迟判定 = HEALTHY (lag_sec=$LAG，健康区间 0~75 秒振荡)"
     elif [ "$LAG" -le 180 ]; then
       echo "延迟判定 = SLOW (lag_sec=$LAG) —— 警告但不计入失败，可能正撞上无成交的空分钟"
       echo "                     隔两分钟再跑一次，若仍然 >75 秒则按 坑34 -> 坑35 -> 坑38 排查"
     else
       echo "延迟判定 = STALLED (lag_sec=$LAG)"
       bad "lag_sec=$LAG 已远超窗口周期，链路停摆(坑34/35)"
     fi ;;
esac

# ------------------------------------------------------------ 4 VIEW 与滚动窗口
echo ""
echo "--- [4] VIEW +8 与折线图滚动窗口 ---"
RAW_MAX="$(q "SELECT IFNULL(MAX(window_end),'NULL') FROM dws_category_sales;")"
VIEW_MAX="$(q "SELECT IFNULL(MAX(window_end),'NULL') FROM v_dws_category_sales;")"
echo "物理表 max(UTC)      = $RAW_MAX"
echo "VIEW   max(北京时间) = $VIEW_MAX"
DIFF_H="$(q "SELECT TIMESTAMPDIFF(HOUR, (SELECT MAX(window_end) FROM dws_category_sales), (SELECT MAX(window_end) FROM v_dws_category_sales));")"
echo "VIEW - 物理表        = ${DIFF_H:-N/A} 小时（必须是 8，见 README 6.5）"
case "$DIFF_H" in
  8) : ;;
  *) bad "VIEW 的偏移是 '${DIFF_H:-空}' 小时而不是 8 小时。折线图时间轴会整体错位（坑 20）" ;;
esac
q "SELECT CONCAT('最近 ${WINDOW_MIN} 分钟: rows=', COUNT(*), ' points=', COUNT(DISTINCT window_end),
                ' from=', IFNULL(MIN(window_end),'-'), ' to=', IFNULL(MAX(window_end),'-'))
   FROM v_dws_category_sales
   WHERE window_end >= DATE_SUB(NOW(), INTERVAL ${WINDOW_MIN} MINUTE);"
echo "（这就是折线图当前能看到的数据。链路中途恢复时左半段有空洞属正常，会逐渐填满）"

# ------------------------------------------------------------ 5 图表时间范围一致性
echo ""
echo "--- [5] Superset 图表时间范围（4 处必须一致，坑 37） ---"
SUPERSET_DB="$SUPERSET_DB" python3 - <<'PY'
import json
import os
import sqlite3
import sys

db = os.environ.get("SUPERSET_DB", os.path.expanduser("~/superset.db"))
if not os.path.isfile(db):
    print("  superset.db 不存在: %s" % db)
    sys.exit(0)

conn = sqlite3.connect(db)
rows = conn.execute("SELECT id, slice_name, params, query_context FROM slices").fetchall()
if not rows:
    print("  slices 表为空")
    sys.exit(0)

bad = 0
for sid, name, params, qc_raw in rows:
    try:
        p = json.loads(params) if params else {}
        qc = json.loads(qc_raw) if qc_raw else {}
    except Exception as exc:
        print("  slice id=%s %s: 解析失败 %s" % (sid, name, exc))
        bad += 1
        continue
    vals = []
    if p.get("time_range"):
        vals.append(p["time_range"])
    for f in p.get("adhoc_filters") or []:
        if f.get("operator") == "TEMPORAL_RANGE" and f.get("comparator"):
            vals.append(f["comparator"])
    fd = qc.get("form_data") or {}
    if fd.get("time_range"):
        vals.append(fd["time_range"])
    for f in fd.get("adhoc_filters") or []:
        if f.get("operator") == "TEMPORAL_RANGE" and f.get("comparator"):
            vals.append(f["comparator"])
    for qq in qc.get("queries") or []:
        for f in qq.get("filters") or []:
            if f.get("op") == "TEMPORAL_RANGE" and f.get("val"):
                vals.append(f["val"])
    if not vals:
        print("  slice id=%-3s %-24s 无时间范围（No filter，看不到实时效果，坑 27）" % (sid, name))
        bad += 1
        continue
    uniq = sorted(set(vals))
    if len(uniq) != 1:
        print("  slice id=%-3s %-24s MISMATCH  -> %s" % (sid, name, uniq))
        bad += 1
        continue
    val = uniq[0]
    if val.strip().lower() == "no filter":
        # 一致但等于查全表历史：累计类图表没问题，趋势类图表会失去实时观感（坑 27）
        print("  slice id=%-3s %-24s CONSISTENT (%d 处) No filter"
              "   <- 全表历史；累计类图表可以，趋势类图表建议改滚动窗口" % (sid, name, len(vals)))
    else:
        print("  slice id=%-3s %-24s CONSISTENT (%d 处) %s" % (sid, name, len(vals), val))
print("  不一致/缺失的图表数 = %d" % bad)
PY

# ------------------------------------------------------------ 6 时间粒度表达式完整性
echo ""
echo "--- [6] 时间粒度表达式完整性（sqlglot 往返，坑 36 / README 6.9） ---"
SUP_CFG="${SUPERSET_CONFIG:-$HOME/superset_config.py}"
SUP_PY="${SUPERSET_PY:-$HOME/superset-env/bin/python}"
if [ ! -f "$SUP_CFG" ]; then
  echo "  找不到配置文件 $SUP_CFG（可用 SUPERSET_CONFIG=... 指定）"
  bad "找不到 superset_config.py，无法校验时间粒度表达式(坑36)"
elif [ ! -x "$SUP_PY" ]; then
  echo "  找不到 $SUP_PY（可用 SUPERSET_PY=... 指定），跳过本节"
elif ! "$SUP_PY" -c 'import sqlglot' >/dev/null 2>&1; then
  echo "  该 Python 环境里没有 sqlglot，跳过本节"
else
  RES="$(SUP_CFG="$SUP_CFG" "$SUP_PY" - 2>/dev/null <<'PY'
import os
import sys

cfg = os.environ["SUP_CFG"]
ns = {}
try:
    with open(cfg, encoding="utf-8") as f:
        exec(compile(f.read(), cfg, "exec"), ns)
except Exception as exc:
    print("ERR 配置文件解析失败: %s" % exc)
    sys.exit(0)

addon = (ns.get("TIME_GRAIN_ADDON_EXPRESSIONS") or {}).get("mysql") or {}
missing = [k for k in ("PT1S", "PT1M", "PT1H", "P1D") if k not in addon]
if missing:
    print("MISSING %s" % ",".join(missing))
    sys.exit(0)

try:
    import sqlglot
except Exception as exc:
    print("ERR import sqlglot 失败: %s" % exc)
    sys.exit(0)

LIT = "'2026-09-04 13:07:42'"
for k in sorted(addon):
    tmpl = addon[k]
    if not isinstance(tmpl, str) or "{col}" not in tmpl:
        continue
    sql = "SELECT %s AS t FROM v_dws_category_sales" % tmpl.format(col=LIT)
    try:
        out = sqlglot.parse_one(sql, dialect="mysql").sql(dialect="mysql")
    except Exception as exc:
        print("ERR %s 解析失败: %s" % (k, exc))
        sys.exit(0)
    if out.upper().count("DATE(") < sql.upper().count("DATE("):
        print("MANGLED %s" % k)
        sys.exit(0)

# 输出 PT1M 经 sqlglot 往返后的表达式，交给 MySQL 实测求值
node = sqlglot.parse_one(
    "SELECT %s AS t" % addon["PT1M"].format(col=LIT), dialect="mysql"
)
print("OK %s" % node.expressions[0].this.sql(dialect="mysql"))
PY
  )"
  if [ -z "$RES" ]; then
    echo "  检查脚本无输出（$SUP_PY 异常？）"
    bad "时间粒度完整性检查无法执行，请用 SUPERSET_PY 指定装有 sqlglot 的解释器"
  else
    VERDICT="${RES%% *}"
    DETAIL="${RES#* }"
    case "$VERDICT" in
      OK)
        echo "  TIME_GRAIN_ADDON_EXPRESSIONS['mysql'] 已配置，全部粒度通过 sqlglot 往返（DATE() 未被删除）"
        GOT="$(q "SELECT ${DETAIL};")"
        echo "  PT1M 经 sqlglot 往返后在 MySQL 求值 = ${GOT:-N/A}    期望 = 2026-09-04 13:07:00"
        [ "$GOT" = "2026-09-04 13:07:00" ] || \
          bad "PT1M 表达式求值结果是 '${GOT}' 而不是 2026-09-04 13:07:00，折线图时间轴会错位(坑36/README 6.9)"
        ;;
      MISSING)
        echo "  配置里缺这些粒度: $DETAIL"
        bad "superset_config.py 缺 TIME_GRAIN_ADDON_EXPRESSIONS['mysql'] 的 $DETAIL —— 折线图会显示未来时间(坑36)，照 README 6.2 补齐后重启 Superset"
        ;;
      MANGLED)
        echo "  被 sqlglot 破坏的粒度: $DETAIL"
        bad "时间粒度表达式 $DETAIL 被 sqlglot 删掉了 DATE()，折线图会显示未来时间(坑36/README 6.9)"
        ;;
      *)
        echo "  $RES"
        bad "时间粒度完整性检查失败: $DETAIL"
        ;;
    esac
  fi
fi

# ------------------------------------------------------------ 7 内存
echo ""
echo "--- [7] 内存 ---"
free -h | head -3
AVAIL_MB="$(free -m | awk '/^Mem:/{print $7}')"; AVAIL_MB="${AVAIL_MB:-0}"
SWAP_MB="$(free -m | awk '/^Swap:/{print $4}')";  SWAP_MB="${SWAP_MB:-0}"
echo "available=${AVAIL_MB}MB  swap_free=${SWAP_MB}MB"
[ "$AVAIL_MB" -ge 1000 ] || bad "available 只剩 ${AVAIL_MB}MB，TaskManager 随时会被饿死(坑35)"
[ "$SWAP_MB"  -ge 200 ]  || bad "swap 只剩 ${SWAP_MB}MB，已濒临 OOM(坑35)"

# ------------------------------------------------------------ 结论
echo ""
echo "============================================================"
if [ -z "$FAILS" ]; then
  echo " 结论: PASS  链路健康，折线图最右端应贴着当前分钟"
  echo "       浏览器按 Ctrl+Shift+R 硬刷新即可（图表是快照，坑 29）"
else
  echo " 结论: FAIL  发现以下问题:"
  printf '%s' "$FAILS" | sed '/^$/d' | sed 's/^/   - /'
fi
echo "============================================================"
[ -z "$FAILS" ] || exit 1
