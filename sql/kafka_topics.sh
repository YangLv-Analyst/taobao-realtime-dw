# ============================================================
# Kafka 主题创建命令
# 环境: Kafka 4.3.1 KRaft 模式
# ============================================================

# 原始订单主题
bin/kafka-topics.sh --create --topic taobao_orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# DWS 聚合结果主题
bin/kafka-topics.sh --create --topic taobao_dws_category_sales --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
