# Data-Flush: MySQL Binlog 工具集

这是一个基于Java开发的MySQL binlog解析和处理工具集，包含数据回滚工具和binlog位置查找工具，可用于数据恢复和故障分析场景。

## 功能特点

### 数据回滚工具 (BinlogRollbackTool)

- 解析MySQL binlog文件
- 支持解析INSERT、UPDATE、DELETE等DML操作
- 自动生成反向SQL（将INSERT转为DELETE，UPDATE转为反向UPDATE，DELETE转为INSERT）
- 支持按数据库、表名过滤
- 支持指定binlog位置范围
- 生成可执行的SQL回滚脚本

### Binlog位置查找工具 (BinlogPositionFinder)

- 根据时间范围查找对应的binlog位置
- 从后向前查找，适合需要回滚到某个时间点的场景
- 支持按数据库、表名过滤
- 自动输出可用于数据回滚的命令

## 环境要求

- Java 11 或更高版本
- Maven 3.6 或更高版本
- MySQL 5.7 或更高版本

## 编译方法

```bash
mvn clean package
```

编译成功后，将在`target`目录下生成可执行的JAR文件：`data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar`

## 使用方法

### 数据回滚工具

```bash
java -jar target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar [选项]
```

#### 命令行选项

| 选项 | 长选项 | 描述 | 是否必需 |
|------|--------|------|----------|
| -h | --host | MySQL主机地址 | 是 |
| -P | --port | MySQL端口 | 否，默认3306 |
| -u | --user | MySQL用户名 | 是 |
| -p | --password | MySQL密码 | 是 |
| -f | --file | Binlog文件名 | 否，不提供则从当前位置开始 |
| -s | --start-position | 开始位置 | 否，默认0（当前位置） |
| -d | --databases | 数据库名称(逗号分隔) | 否 |
| -t | --tables | 表名称(逗号分隔) | 否 |
| -o | --output | 输出文件路径 | 否，默认rollback.sql |
| --help | | 显示帮助信息 | 否 |

#### 使用示例

```bash
# 解析指定binlog文件并生成回滚SQL
java -jar target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -h localhost -P 3306 -u root -p password \
  -f mysql-bin.000001 -o rollback.sql

# 指定位置范围和数据库过滤
java -jar target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -h localhost -u root -p password \
  -f mysql-bin.000001 -s 4 \
  -d mydb -t users,orders -o rollback.sql
```

### Binlog位置查找工具

```bash
java -cp target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar com.dataflush.BinlogPositionFinder [选项]
```

#### 命令行选项

| 选项 | 长选项 | 描述 | 是否必需 |
|------|--------|------|----------|
| -h | --host | MySQL主机地址 | 是 |
| -P | --port | MySQL端口 | 否，默认3306 |
| -u | --user | MySQL用户名 | 是 |
| -p | --password | MySQL密码 | 是 |
| -st | --start-time | 开始时间 (格式: yyyy-MM-dd HH:mm:ss) | 否* |
| -et | --end-time | 结束时间 (格式: yyyy-MM-dd HH:mm:ss) | 否* |
| -d | --databases | 数据库名称(逗号分隔) | 否 |
| -t | --tables | 表名称(逗号分隔) | 否 |
| --help | | 显示帮助信息 | 否 |

*注：必须至少指定开始时间或结束时间其中之一

#### 使用示例

```bash
# 查找指定时间范围内的binlog位置
java -cp target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar com.dataflush.BinlogPositionFinder \
  -h localhost -P 3306 -u root -p password \
  -st "2023-01-01 00:00:00" -et "2023-01-01 12:00:00"

# 指定数据库和表过滤
java -cp target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar com.dataflush.BinlogRollbackTool \
  -h localhost -u root -p password \
  -st "2023-01-01 00:00:00" -d mydb -t users,orders
```

## 工作原理

### 数据回滚工具

1. 连接MySQL并读取指定的binlog文件
2. 解析binlog中的事件（TableMap, WriteRows, UpdateRows, DeleteRows）
3. 根据事件类型生成反向SQL：
   - INSERT事件 → 生成DELETE语句
   - UPDATE事件 → 生成反向UPDATE语句
   - DELETE事件 → 生成INSERT语句
4. 将生成的SQL写入输出文件

### Binlog位置查找工具

1. 连接MySQL并获取可用的binlog文件列表
2. 从最新的binlog文件开始，逐个分析文件的时间范围
3. 对于时间范围包含目标时间的文件，使用顺序查找法定位精确位置
4. 找到匹配的位置后，输出可用于数据回滚的命令

## 注意事项

1. 使用此工具需要有MySQL的binlog访问权限
2. 建议在执行回滚SQL前先备份数据库
3. 对于没有主键的表，回滚可能不够精确，建议所有表都有主键
4. 时间查找功能依赖于binlog中记录的事件时间戳，确保MySQL服务器时间准确

## 依赖项

- MySQL Binlog Connector (com.zendesk:mysql-binlog-connector-java:0.30.1)
- MySQL JDBC Driver (mysql:mysql-connector-java:8.0.28)
- Apache Commons CLI (commons-cli:commons-cli:1.5.0)
- SLF4J & Logback (用于日志记录)
- Jackson (用于JSON处理)

## 许可证

MIT License