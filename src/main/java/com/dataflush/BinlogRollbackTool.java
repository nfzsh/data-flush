package com.dataflush;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.apache.commons.cli.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MySQL Binlog解析和反向SQL生成工具
 * 用于数据回滚操作
 */
public class BinlogRollbackTool {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BinlogRollbackTool.class);

    /**
     * 初始化输出文件，写入文件头信息
     */
    private static void initOutputFile(String outputFile, String binlogFile) {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(outputFile))) {
            writer.println("-- 数据回滚SQL脚本");
            writer.println("-- 生成时间: " + new Date());
            if (binlogFile != null && !binlogFile.isEmpty()) {
                writer.println("-- Binlog文件: " + binlogFile);
            }
            writer.println("-- 实时监控模式");
            writer.println();
        } catch (IOException e) {
            logger.error("初始化输出文件失败", e);
        }
    }

    public static void main(String[] args) {
        // 定义命令行选项
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("host").hasArg().desc("MySQL主机地址").required().build());
        options.addOption(Option.builder("P").longOpt("port").hasArg().desc("MySQL端口").type(Number.class).build());
        options.addOption(Option.builder("u").longOpt("user").hasArg().desc("MySQL用户名").required().build());
        options.addOption(Option.builder("p").longOpt("password").hasArg().desc("MySQL密码").required().build());
        options.addOption(Option.builder("f").longOpt("file").hasArg().desc("Binlog文件名").build());
        options.addOption(Option.builder("s").longOpt("start-position").hasArg().desc("开始位置").type(Number.class).build());
        options.addOption(Option.builder("d").longOpt("databases").hasArg().desc("数据库名称(逗号分隔)").build());
        options.addOption(Option.builder("t").longOpt("tables").hasArg().desc("表名称(逗号分隔)").build());
        options.addOption(Option.builder("o").longOpt("output").hasArg().desc("输出文件路径").build());
        options.addOption(Option.builder("help").desc("显示帮助信息").build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help")) {
                formatter.printHelp("BinlogRollbackTool", options);
                return;
            }

            // 解析命令行参数
            String host = cmd.getOptionValue("host");
            int port = Integer.parseInt(cmd.getOptionValue("port", "3306"));
            String user = cmd.getOptionValue("user");
            String password = cmd.getOptionValue("password");
            String binlogFile = cmd.getOptionValue("file", ""); // 可选参数，如果不提供则从当前位置开始
            long startPosition = Long.parseLong(cmd.getOptionValue("start-position", "0")); // 默认从当前位置开始，0表示使用服务器当前位置

            Set<String> databases = new HashSet<>();
            if (cmd.hasOption("databases")) {
                databases.addAll(Arrays.asList(cmd.getOptionValue("databases").split(",")));
            }
            
            Set<String> tables = new HashSet<>();
            if (cmd.hasOption("tables")) {
                tables.addAll(Arrays.asList(cmd.getOptionValue("tables").split(",")));
            }
            
            String outputFile = cmd.getOptionValue("output", "rollback.sql");
            
            // 初始化输出文件
            if (outputFile != null && !outputFile.isEmpty()) {
                initOutputFile(outputFile, binlogFile);
            }

            // 创建BinlogProcessor实例并处理binlog
            BinlogProcessor processor = new BinlogProcessor(host, port, user, password);
            processor.processBinlog(binlogFile, startPosition, databases, tables, outputFile);

        } catch (ParseException e) {
            System.err.println("参数解析错误: " + e.getMessage());
            formatter.printHelp("BinlogRollbackTool", options);
        } catch (Exception e) {
            logger.error("处理binlog时发生错误", e);
        }
    }

    /**
     * Binlog处理器类
     */
    private static class BinlogProcessor {
        private final String host;
        private final int port;
        private final String user;
        private final String password;
        private final Map<String, TableMetadata> tableMetadataCache = new HashMap<>();

        public BinlogProcessor(String host, int port, String user, String password) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
        }

        /**
         * 处理binlog文件并生成反向SQL
         */
        public void processBinlog(String binlogFile, long startPosition,
                                 Set<String> databases, Set<String> tables, String outputFile) throws Exception {
            
            logger.info("开始实时监控binlog文件: {}, 起始位置: {}", binlogFile.isEmpty() ? "当前服务器binlog文件" : binlogFile, startPosition > 0 ? startPosition : "当前位置");
            
            BinaryLogClient client = new BinaryLogClient(host, port, user, password);
            
            // 设置初始binlog文件名，如果未指定则使用服务器当前文件
            if (binlogFile != null && !binlogFile.isEmpty()) {
                client.setBinlogFilename(binlogFile);
            }
            
            // 设置初始位置，如果指定了大于0的位置则使用指定位置，否则使用服务器当前位置
            if (startPosition > 0) {
                client.setBinlogPosition(startPosition);
            }
            
            EventDeserializer eventDeserializer = new EventDeserializer();
            eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
            );
            client.setEventDeserializer(eventDeserializer);
            
            // 创建文件写入器（如果需要输出到文件）
            PrintWriter fileWriter = null;
            if (outputFile != null && !outputFile.isEmpty()) {
                try {
                    fileWriter = new PrintWriter(new FileOutputStream(outputFile, true));
                } catch (IOException e) {
                    logger.error("创建输出文件写入器失败", e);
                }
            }
            
            // 当前表映射信息
            AtomicReference<TableMapEventData> currentTableMap = new AtomicReference<>();
            
            // 保存文件写入器的最终引用，以便在lambda表达式中使用
            final PrintWriter finalFileWriter = fileWriter;
            
            client.registerEventListener(event -> {
                EventData data = event.getData();
                // 移除位置范围限制，持续监控所有事件
                
                // 处理表映射事件
                if (data instanceof TableMapEventData) {
                    currentTableMap.set((TableMapEventData) data);
                    String database = currentTableMap.get().getDatabase();
                    String table = currentTableMap.get().getTable();

                    // 检查是否需要处理该表
                    boolean shouldProcess = (!databases.isEmpty() && databases.contains(database))
                            && (!tables.isEmpty() && tables.contains(table));
                    logger.debug("表过滤检查: {}.{} -> {}", database, table, shouldProcess);
                    if (!shouldProcess) {
                        return;
                    }

                    // 加载表元数据
                    try {
                        loadTableMetadata(database, table);
                    } catch (Exception e) {
                        logger.error("加载表元数据失败: {}.{}", database, table, e);
                    }
                }
                
                // 处理写入事件(INSERT)
                else if (data instanceof WriteRowsEventData) {
                    if (currentTableMap.get() == null) {
                        return;
                    }
                    
                    WriteRowsEventData writeData = (WriteRowsEventData) data;
                    String database = currentTableMap.get().getDatabase();
                    String table = currentTableMap.get().getTable();
                    
                    // 检查是否需要处理该表
                    boolean shouldProcess = (!databases.isEmpty() && databases.contains(database))
                            && (!tables.isEmpty() && tables.contains(table));
                    logger.debug("表过滤检查: {}.{} -> {}", database, table, shouldProcess);
                    if (!shouldProcess) {
                        return;
                    }
                    
                    TableMetadata metadata = tableMetadataCache.get(database + "." + table);
                    if (metadata == null) {
                        logger.warn("未找到表元数据: {}.{}", database, table);
                        return;
                    }
                    
                    // 生成DELETE语句作为INSERT的回滚并实时打印
                    for (Object[] row : writeData.getRows()) {
                        String deleteSql = generateDeleteSql(database, table, metadata, row);
                        logger.info("生成回滚SQL(INSERT->DELETE): {};", deleteSql);
                        
                        // 如果提供了输出文件，则同时写入文件
                        if (finalFileWriter != null) {
                            finalFileWriter.println(deleteSql + ";");
                            finalFileWriter.flush();
                        }
                    }
                }
                
                // 处理更新事件(UPDATE)
                else if (data instanceof UpdateRowsEventData) {
                    if (currentTableMap.get() == null) {
                        return;
                    }
                    
                    UpdateRowsEventData updateData = (UpdateRowsEventData) data;
                    String database = currentTableMap.get().getDatabase();
                    String table = currentTableMap.get().getTable();
                    
                    // 检查是否需要处理该表
                    boolean shouldProcess = (!databases.isEmpty() && databases.contains(database))
                            && (!tables.isEmpty() && tables.contains(table));
                    logger.debug("表过滤检查: {}.{} -> {}", database, table, shouldProcess);
                    if (!shouldProcess) {
                        return;
                    }
                    
                    TableMetadata metadata = tableMetadataCache.get(database + "." + table);
                    if (metadata == null) {
                        logger.warn("未找到表元数据: {}.{}", database, table);
                        return;
                    }
                    
                    // 生成反向UPDATE语句并实时打印
                    for (Map.Entry<Serializable[], Serializable[]> entry : updateData.getRows()) {
                        Serializable[] rowBefore = entry.getKey();
                        Serializable[] rowAfter = entry.getValue();
                        String updateSql = generateUpdateSql(database, table, metadata, rowAfter, rowBefore);
                        logger.info("生成回滚SQL(UPDATE->UPDATE): {};", updateSql);
                        
                        // 如果提供了输出文件，则同时写入文件
                        if (finalFileWriter != null) {
                            finalFileWriter.println(updateSql + ";");
                            finalFileWriter.flush();
                        }
                    }
                }
                
                // 处理删除事件(DELETE)
                else if (data instanceof DeleteRowsEventData) {
                    if (currentTableMap.get() == null) {
                        return;
                    }
                    
                    DeleteRowsEventData deleteData = (DeleteRowsEventData) data;
                    String database = currentTableMap.get().getDatabase();
                    String table = currentTableMap.get().getTable();
                    
                    // 检查是否需要处理该表
                    boolean shouldProcess = (!databases.isEmpty() && databases.contains(database))
                            && (!tables.isEmpty() && tables.contains(table));
                    logger.debug("表过滤检查: {}.{} -> {}", database, table, shouldProcess);
                    if (!shouldProcess) {
                        return;
                    }
                    
                    TableMetadata metadata = tableMetadataCache.get(database + "." + table);
                    if (metadata == null) {
                        logger.warn("未找到表元数据: {}.{}", database, table);
                        return;
                    }
                    
                    // 生成INSERT语句作为DELETE的回滚并实时打印
                    for (Object[] row : deleteData.getRows()) {
                        String insertSql = generateInsertSql(database, table, metadata, row);
                        logger.info("生成回滚SQL(DELETE->INSERT): {};", insertSql);
                        
                        // 如果提供了输出文件，则同时写入文件
                        if (finalFileWriter != null) {
                            finalFileWriter.println(insertSql + ";");
                            finalFileWriter.flush();
                        }
                    }
                }
            });
            
            // 连接并持续监控binlog
            logger.info("开始连接MySQL并监控binlog变更...");
            try {
                client.connect();
                
                // 连接成功后，获取当前binlog文件和位置
                if (startPosition <= 0) {
                    String currentBinlogFilename = client.getBinlogFilename();
                    long currentPosition = client.getBinlogPosition();
                    logger.info("连接成功，当前binlog文件: {}, 位置: {}", currentBinlogFilename, currentPosition);
                    
                    // 更新输出文件中的binlog文件信息
                    if (finalFileWriter != null) {
                        finalFileWriter.println("-- 实际监控的Binlog文件: " + currentBinlogFilename);
                        finalFileWriter.println("-- 起始位置: " + currentPosition);
                        finalFileWriter.println();
                        finalFileWriter.flush();
                    }
                }
                
                // 添加关闭钩子，确保程序退出时能够正常断开连接
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        logger.info("正在断开MySQL连接...");
                        client.disconnect();
                    } catch (IOException e) {
                        logger.error("断开连接时发生错误", e);
                    }
                }));
                
                // 主线程等待，保持程序运行
                logger.info("监控已启动，按Ctrl+C停止...");
                Thread.currentThread().join();
                
            } catch (Exception e) {
                logger.error("监控过程中发生错误", e);
                try {
                    client.disconnect();
                } catch (IOException ex) {
                    logger.error("断开连接时发生错误", ex);
                }
            } finally {
                // 关闭文件写入器
                if (finalFileWriter != null) {
                    finalFileWriter.close();
                }
            }
        }

        /**
         * 加载表元数据信息
         */
        private void loadTableMetadata(String database, String table) throws Exception {
            String key = database + "." + table;
            if (tableMetadataCache.containsKey(key)) {
                return;
            }
            
            String url = "jdbc:mysql://" + host + ":" + port + "/" + database;
            try (Connection conn = DriverManager.getConnection(url, user, password);
                 Statement stmt = conn.createStatement()) {
                
                // 获取表结构信息
                ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + table);
                List<String> columns = new ArrayList<>();
                List<String> primaryKeys = new ArrayList<>();
                
                while (rs.next()) {
                    String column = rs.getString("Field");
                    String index = rs.getString("Key");
                    columns.add(column);
                    
                    if ("PRI".equalsIgnoreCase(index)) {
                        primaryKeys.add(column);
                    }
                }
                
                // 如果没有找到主键，尝试获取主键信息
                if (primaryKeys.isEmpty()) {
                    // 增强主键检测：使用information_schema查询
                    String pkQuery = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE " +
                            "WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + "' " +
                            "AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION";
                    try (ResultSet pkRs = stmt.executeQuery(pkQuery)) {
                        while (pkRs.next()) {
                            primaryKeys.add(pkRs.getString("COLUMN_NAME"));
                        }
                    } catch (SQLException e) {
                        logger.warn("主键查询失败，尝试SHOW INDEX方法", e);
                        rs = stmt.executeQuery("SHOW INDEX FROM " + table + " WHERE Key_name = 'PRIMARY'");
                        while (rs.next()) {
                            primaryKeys.add(rs.getString("Column_name"));
                        }
                    }
                }
                
                TableMetadata metadata = new TableMetadata(columns, primaryKeys);
                tableMetadataCache.put(key, metadata);
                
                logger.info("已加载表元数据: {}.{}, 列数: {}, 主键: {}", 
                        database, table, (Integer) columns.size(), String.join(", ", primaryKeys));
            }
        }

        /**
         * 生成DELETE语句（用于回滚INSERT）
         */
        private String generateDeleteSql(String database, String table, TableMetadata metadata, Object[] row) {
            StringBuilder sql = new StringBuilder();
            sql.append("DELETE FROM `").append(database).append("`.`").append(table).append("` WHERE ");
            
            List<String> whereClauses = new ArrayList<>();
            List<String> columns = metadata.getColumns();
            List<String> primaryKeys = metadata.getPrimaryKeys();
            
            // 优先使用主键作为WHERE条件
            if (!primaryKeys.isEmpty()) {
                for (String pk : primaryKeys) {
                    int index = columns.indexOf(pk);
                    if (index >= 0 && index < row.length) {
                        whereClauses.add("`" + pk + "` = " + formatValue(row[index]));
                    }
                }
            } 
            // 如果没有主键，使用所有列作为条件
            else {
                for (int i = 0; i < columns.size() && i < row.length; i++) {
                    String column = columns.get(i);
                    whereClauses.add("`" + column + "` = " + formatValue(row[i]));
                }
            }
            
            sql.append(String.join(" AND ", whereClauses));
            return sql.toString();
        }

        /**
         * 生成INSERT语句（用于回滚DELETE）
         */
        private String generateInsertSql(String database, String table, TableMetadata metadata, Object[] row) {
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO `").append(database).append("`.`").append(table).append("` (");
            
            List<String> columns = metadata.getColumns();
            List<String> columnNames = new ArrayList<>();
            List<String> values = new ArrayList<>();
            
            for (int i = 0; i < columns.size() && i < row.length; i++) {
                columnNames.add("`" + columns.get(i) + "`");
                values.add(formatValue(row[i]));
            }
            
            sql.append(String.join(", ", columnNames));
            sql.append(") VALUES (");
            sql.append(String.join(", ", values));
            sql.append(")");
            
            return sql.toString();
        }

        /**
         * 生成UPDATE语句（用于回滚UPDATE）
         */
        private String generateUpdateSql(String database, String table, TableMetadata metadata,
                                         Serializable[] rowAfter, Serializable[] rowBefore) {
            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE `").append(database).append("`.`").append(table).append("` SET ");
            
            List<String> setClauses = new ArrayList<>();
            List<String> whereClauses = new ArrayList<>();
            List<String> columns = metadata.getColumns();
            List<String> primaryKeys = metadata.getPrimaryKeys();
            
            // 设置SET子句（使用旧值）
            for (int i = 0; i < columns.size() && i < rowBefore.length && i < rowAfter.length; i++) {
                String column = columns.get(i);
                // 只更新发生变化的列
                String oldValue = formatValue(rowBefore[i]);
                String newValue = formatValue(rowAfter[i]);
                if (!Objects.equals(oldValue, newValue)) {
                    setClauses.add("`" + column + "` = " + oldValue);
                }
            }
            
            // 设置WHERE子句（使用新值的主键）
            if (!primaryKeys.isEmpty()) {
                for (String pk : primaryKeys) {
                    int index = columns.indexOf(pk);
                    if (index >= 0 && index < rowAfter.length) {
                        whereClauses.add("`" + pk + "` = " + formatValue(rowAfter[index]));
                    }
                }
            } 
            // 如果没有主键，使用所有列作为条件
            else {
                for (int i = 0; i < columns.size() && i < rowAfter.length; i++) {
                    String column = columns.get(i);
                    whereClauses.add("`" + column + "` = " + formatValue(rowAfter[i]));
                }
            }
            
            sql.append(String.join(", ", setClauses));
            sql.append(" WHERE ");
            sql.append(String.join(" AND ", whereClauses));
            
            return sql.toString();
        }

        /**
         * 格式化SQL值
         */
        private String formatValue(Object value) {
            if (value == null) {
                return "NULL";
            } else if (value instanceof String || value instanceof byte[]) {
                String strValue;
                if (value instanceof byte[]) {
                    strValue = new String((byte[]) value);
                } else {
                    strValue = value.toString();
                }
                // 转义单引号
                strValue = strValue.replace("'", "\\'");
                return "'" + strValue + "'";
            } else if (value instanceof Boolean) {
                return (Boolean) value ? "1" : "0";
            } else if (value instanceof Date) {
                return "'" + value.toString() + "'";
            } else {
                return value.toString();
            }
        }
    }

    /**
     * 表元数据类
     */
    private static class TableMetadata {
        private final List<String> columns;
        private final List<String> primaryKeys;

        public TableMetadata(List<String> columns, List<String> primaryKeys) {
            this.columns = columns;
            this.primaryKeys = primaryKeys;
        }

        public List<String> getColumns() {
            return columns;
        }

        public List<String> getPrimaryKeys() {
            return primaryKeys;
        }
    }
}