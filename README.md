# sclog_lite

`sclog_lite` 是一个基于第三方优秀日志库 `loguru` 的轻量级 Python 日志库。它在保持 `loguru` 极其简单易用、功能强大的特性的同时，扩展了**异步、连接池化、批量、高隔离**的 MySQL 日志持久化写入功能。

---

## 💡 特性

1. **原汁原味的 loguru 体验**：完全保留 `loguru` 的控制台和文件输出特性，使用习惯没有任何改变。
2. **异步批量持久化**：采用后台守护线程配合线程安全 FIFO 队列，支持最大批次大小（`batch_size`）和最长刷新间隔（`batch_interval`）双重机制触发批量写入。
3. **数据库连接池支持**：基于 `DBUtils` 提供的 `PooledDB` 数据库连接池和 `pymysql` 底层驱动，保证高并发场景下的资源重用和高性能。
4. **全面的写入失败隔离**：
   - 数据库连接断开、查询错误、建表失败等内部数据库异常**绝对不会**阻塞或使主应用程序崩溃（失败隔离）。
   - 支持本地 JSON-lines 格式备用容灾文件（`fallback_path`），在数据库故障期间自动将日志转储到本地，故障恢复后数据库自动重连恢复写入。
   - 队列满隔离，在队列爆满时可选择丢弃、报错或自动转储本地。

---

## 🛠️ 安装

```bash
# 进入目录后本地安装
pip install -e .
```

---

## 🚀 快速上手

### 1. 标准控制台和文件日志输出（与 loguru 体验一致）

```python
from sclog_lite import logger, setup_logging

# 1. 快速配置：控制台输出等级为 INFO，同时输出到文件 (支持日志滚动与定期清理)
setup_logging(
    console=True,
    console_level="INFO",
    file_path="app.log",
    file_level="DEBUG",
    file_rotation="10 MB",
    file_retention="10 days"
)

# 2. 正常打印日志
logger.debug("这是一条调试日志")
logger.info("这是一条普通信息日志")
logger.warning("这是一条警告日志")
logger.error("这是一条错误日志")
```

### 2. 输出日志到 MySQL 数据库

您可以使用 `add_mysql_sink` 函数（或直接调用已绑定的 `logger.add_mysql_sink` 方法）轻松增加 MySQL 日志接收器。

```python
import time
from sclog_lite import logger

# 添加 MySQL 日志接收器
logger.add_mysql_sink(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="yourpassword",
    database="test_db",
    table_name="my_app_logs",   # 自定义表名，默认 "log_entries"
    auto_create_table=True,      # 自动创建结构一致的日志表
    batch_size=50,               # 每满 50 条日志自动批量写入一次
    batch_interval=1.0,          # 如果 1.0 秒内未满 50 条，也会强制刷新写入
    fallback_path="mysql_fallback.jsonl"  # 数据库连接失败时的本地容灾日志文件
)

# 像往常一样记录日志
for i in range(100):
    logger.info(f"第 {i} 条性能测试日志")
    time.sleep(0.01)

# 注意：在主程序结束时，sclog_lite 会自动触发优雅停机(Graceful Shutdown)，
# 清空队列中剩余的日志并写回数据库，同时关闭数据库连接池。
```

---

## ⚙️ 核心参数说明

| 参数名称 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `host` | `str` | *必填* | MySQL 数据库主机地址 |
| `port` | `int` | *必填* | MySQL 数据库端口号 |
| `user` | `str` | *必填* | MySQL 数据库用户名 |
| `password` | `str` | *必填* | MySQL 数据库密码 |
| `database` | `str` | *必填* | MySQL 数据库名称 |
| `table_name` | `str` | `"log_entries"` | 日志保存的表名 |
| `auto_create_table`| `bool` | `True` | 如果表不存在，是否自动创建默认结构的日志表 |
| `batch_size` | `int` | `100` | 触发批量写入数据库的最大日志条数（基于 `executemany`） |
| `batch_interval` | `float`| `1.0` | 触发批量写入的最大等待时间（秒） |
| `queue_maxsize` | `int` | `10000` | 内存中日志缓冲队列的最大长度 |
| `queue_block` | `bool` | `False` | 队列爆满时是否阻塞（建议为 `False` 保证极致的失败隔离） |
| `fallback_path` | `str` | `None` | 本地 JSON-lines 容灾文件路径。在数据库故障或队列爆满时，日志会自动序列化并转储至此文件中 |
| `pool_config` | `dict` | `None` | 额外的 `DBUtils.PooledDB` 连接池配置（支持：`mincached`, `maxcached`, `maxconnections` 等） |

---

## 🧪 运行测试用例

我们提供了 100% 覆盖核心特性（包含批量触发、定时触发、数据库崩溃隔离、本地容灾与池化重用等）的 `pytest` 测试套件：

```bash
# 运行全部测试用例
pytest
```
