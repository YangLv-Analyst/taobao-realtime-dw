"""淘宝订单仿真生产者：泊松到达 + 时段波峰 + 品类加权 + 二八用户 + 1%脏数据"""
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer
KAFKA_SERVER = "127.0.0.1:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),

)

# 仿真参数 1：品类加权（比例参考离线数仓真实分布，Clothing 遥遥领先）
CATEGORIES = ["Clothing", "Electronics", "Home", "Food", "Beauty", "Sports", "Books", "Toys"]
CATEGORY_WEIGHTS = [31, 16, 15, 13, 10, 8, 4, 3]

# 仿真参数 2：品类锚定价格区间（元），符合商业常识
PRICE_RANGE = {
    "Clothing": (50, 500), "Electronics": (200, 2000), "Home": (20, 1500),
    "Food": (5, 200), "Beauty": (30, 600), "Sports": (30, 800),
    "Books": (10, 150), "Toys": (20, 400),
}
PAYMENTS = ["Alipay", "WeChat", "CreditCard", "BankCard"]
PAYMENT_WEIGHTS = [45, 35, 12, 8]          # 支付宝微信占八成，贴合国内现状

# 仿真参数 3：二八定律用户池——500 个用户，头部 100 个是高频买家
HOT_CUSTOMERS = [f"C{i:04d}" for i in range(1, 101)]
ALL_CUSTOMERS = [f"C{i:04d}" for i in range(1, 501)]


def hour_factor():
    """仿真参数 4：时段波峰——凌晨 4 点系数 0.2，晚 9 点系数 2.0"""
    hour = datetime.now().hour
    return 0.2 + 1.8 * max(0, 1 - abs(hour - 21) / 9)


order_id = 100000
print(f"开始生产订单，目标：{KAFKA_SERVER}，Ctrl+C 停止")
try:
    while True:
        order_id += 1
        category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS)[0]
        lo, hi = PRICE_RANGE[category]
        order = {
            "order_id": order_id,
            "customer_id": random.choice(HOT_CUSTOMERS) if random.random() < 0.6 else random.choice(ALL_CUSTOMERS),
            "gender": random.choice(["M", "F"]),
            "age": random.randint(18, 60),
            "category": category,
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(lo, hi), 2),
            "payment_method": random.choices(PAYMENTS, weights=PAYMENT_WEIGHTS)[0],
            "invoice_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }
        # 仿真参数 5：1% 脏数据（0.5% 退款负数量 + 0.5% 价格离群），给下游清洗留实战素材
        r = random.random()
        if r < 0.005:
            order["quantity"] = -order["quantity"]
        elif r < 0.01:
            order["price"] = round(order["price"] * 100, 2)

        producer.send("taobao_orders", value=order)
        print(f"已发送: {order['order_id']} | {order['category']} | qty={order['quantity']} | {order['price']}")

        # 仿真参数 6：泊松到达——间隔服从指数分布，均值随时段波峰缩放
        time.sleep(random.expovariate(hour_factor()))
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    producer.close()
    print("生产者已停止")