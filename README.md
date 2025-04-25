# Data-Flush: MySQL Binlog 回滚工具

这是一个基于Java开发的MySQL binlog解析和数据回滚工具，可以通过解析binlog生成反向SQL语句，用于数据回滚操作。

## 功能特点

- 解析MySQL binlog文件
- 支持解析INSERT、UPDATE、DELETE等DML操作
- 自动生成反向SQL（将INSERT转为DELETE，UPDATE转为反向UPDATE，DELETE转为INSERT）
- 支持按数据库、表名过滤
- 支持指定binlog位置范围
- 生成可执行的SQL回滚脚本

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

```bash
java -jar target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar [选项]
```

### 命令行选项

| 选项 | 长选项 | 描述 | 是否必需 |
|------|--------|------|----------|
| -h | --host | MySQL主机地址 | 是 |
| -P | --port | MySQL端口 | 否，默认3306 |
| -u | --user | MySQL用户名 | 是 |
| -p | --password | MySQL密码 | 是 |
| -f | --file | Binlog文件名 | 是 |
| -s | --start-position | 开始位置 | 否，默认0 |
| -e | --end-position | 结束位置 | 否，默认最大值 |
| -d | --databases | 数据库名称(逗号分隔) | 否 |
| -t | --tables | 表名称(逗号分隔) | 否 |
| -o | --output | 输出文件路径 | 否，默认rollback.sql |
| --help | | 显示帮助信息 | 否 |

### 使用示例

```bash
# 解析指定binlog文件并生成回滚SQL
java -jar target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -h localhost -P 3306 -u root -p password \
  -f mysql-bin.000001 -o rollback.sql

# 指定位置范围和数据库过滤
java -jar target/data-flush-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -h localhost -u root -p password \
  -f mysql-bin.000001 -s 4 -e 1024 \
  -d mydb -t users,orders -o rollback.sql
```

## 注意事项

1. 使用此工具需要有MySQL的binlog访问权限
2. 建议在执行回滚SQL前先备份数据库
3. 生成的回滚SQL按照事件的反序排列，以确保正确的回滚顺序
4. 对于没有主键的表，回滚可能不够精确，建议所有表都有主键

## 工作原理

1. 连接MySQL并读取指定的binlog文件
2. 解析binlog中的事件（TableMap, WriteRows, UpdateRows, DeleteRows）
3. 根据事件类型生成反向SQL：
   - INSERT事件 → 生成DELETE语句
   - UPDATE事件 → 生成反向UPDATE语句
   - DELETE事件 → 生成INSERT语句
4. 反转SQL语句顺序（因为回滚需要从后往前执行）
5. 将生成的SQL写入输出文件

## 许可证

MIT License