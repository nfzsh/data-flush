package com.dataflush;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MySQL Binlog位置查找工具
 * 通过指定时间范围查找对应的binlog位置
 * 从后向前查找，适合需要回滚到某个时间点的场景
 */
public class BinlogPositionFinder {

    private static final Logger logger = LoggerFactory.getLogger(BinlogPositionFinder.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        // 定义命令行选项
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("host").hasArg().desc("MySQL主机地址").required().build());
        options.addOption(Option.builder("P").longOpt("port").hasArg().desc("MySQL端口").type(Number.class).build());
        options.addOption(Option.builder("u").longOpt("user").hasArg().desc("MySQL用户名").required().build());
        options.addOption(Option.builder("p").longOpt("password").hasArg().desc("MySQL密码").required().build());
        options.addOption(Option.builder("st").longOpt("start-time").hasArg().desc("开始时间 (格式: yyyy-MM-dd HH:mm:ss)").build());
        options.addOption(Option.builder("et").longOpt("end-time").hasArg().desc("结束时间 (格式: yyyy-MM-dd HH:mm:ss)").build());
        options.addOption(Option.builder("d").longOpt("databases").hasArg().desc("数据库名称(逗号分隔)").build());
        options.addOption(Option.builder("t").longOpt("tables").hasArg().desc("表名称(逗号分隔)").build());
        options.addOption(Option.builder("help").desc("显示帮助信息").build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help")) {
                formatter.printHelp("BinlogPositionFinder", options);
                return;
            }

            // 解析命令行参数
            String host = cmd.getOptionValue("host");
            int port = Integer.parseInt(cmd.getOptionValue("port", "3306"));
            String user = cmd.getOptionValue("user");
            String password = cmd.getOptionValue("password");

            // 解析时间参数
            Date startTime = null;
            Date endTime = null;

            if (cmd.hasOption("start-time")) {
                try {
                    startTime = DATE_FORMAT.parse(cmd.getOptionValue("start-time"));
                } catch (ParseException e) {
                    System.err.println("开始时间格式错误，请使用 yyyy-MM-dd HH:mm:ss 格式");
                    return;
                }
            }

            if (cmd.hasOption("end-time")) {
                try {
                    endTime = DATE_FORMAT.parse(cmd.getOptionValue("end-time"));
                } catch (ParseException e) {
                    System.err.println("结束时间格式错误，请使用 yyyy-MM-dd HH:mm:ss 格式");
                    return;
                }
            }

            if (startTime == null && endTime == null) {
                System.err.println("必须至少指定开始时间或结束时间");
                return;
            }

            Set<String> databases = new HashSet<>();
            if (cmd.hasOption("databases")) {
                databases.addAll(Arrays.asList(cmd.getOptionValue("databases").split(",")));
            }

            Set<String> tables = new HashSet<>();
            if (cmd.hasOption("tables")) {
                tables.addAll(Arrays.asList(cmd.getOptionValue("tables").split(",")));
            }

            // 创建BinlogPositionFinder实例并查找位置
            BinlogFinder finder = new BinlogFinder(host, port, user, password);
            finder.findPositionByTime(startTime, endTime, databases, tables);

        } catch (org.apache.commons.cli.ParseException e) {
            System.err.println("参数解析错误: " + e.getMessage());
            formatter.printHelp("BinlogPositionFinder", options);
        } catch (Exception e) {
            logger.error("查找binlog位置时发生错误", e);
        }
    }

    /**
     * Binlog位置查找器类
     */
    private static class BinlogFinder {
        private final String host;
        private final int port;
        private final String user;
        private final String password;

        public BinlogFinder(String host, int port, String user, String password) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
        }

        /**
         * 获取MySQL服务器上可用的binlog文件列表
         */
        private Map<String, Long> getBinlogFiles() throws SQLException {
            Map<String, Long> binlogFilesTemp = new LinkedHashMap<>();
            String url = "jdbc:mysql://" + host + ":" + port;
            try (Connection conn = DriverManager.getConnection(url, user, password);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW BINARY LOGS")) {

                while (rs.next()) {
                    binlogFilesTemp.put(rs.getString("Log_name"), rs.getLong("File_size"));
                }
            }
            // 按照文件名倒序排列，从最新的binlog文件开始查找
            List<String> binlogFiles = new ArrayList<>(binlogFilesTemp.keySet());
            binlogFiles.sort(Collections.reverseOrder());
            Map<String, Long> binlogFilesMap = new LinkedHashMap<>();
            binlogFiles.forEach(binlogFile -> binlogFilesMap.put(binlogFile, binlogFilesTemp.get(binlogFile)));
            return binlogFilesMap;
        }

        /**
         * 获取binlog文件的时间范围
         *
         * @param binlogFile binlog文件名
         * @return 包含文件开始和结束时间的对象，如果无法获取则返回null
         */
        private FileTimeRange getFileTimeRange(String binlogFile, boolean isLastFile, FileTimeRange next) {
            FileTimeRange timeRange = new FileTimeRange();
            timeRange.setFileName(binlogFile);
            // 获取文件开始时间（第一个事件的时间）
            EventTimeInfo startTimeInfo = getEventTimeAtPosition(binlogFile, 4); // 从文件头开始
            if (startTimeInfo == null || !startTimeInfo.isFound()) {
                logger.warn("无法获取文件{}的开始时间", binlogFile);
                return null;
            }
            timeRange.setStartTime(startTimeInfo.getEventTime());
            if (isLastFile) {
                timeRange.setEndTime(new Date());
            } else {
                timeRange.setEndTime(next.getStartTime());
            }

            logger.info("文件{}的时间范围: {} 至 {}", binlogFile,
                    DATE_FORMAT.format(timeRange.getStartTime()),
                    DATE_FORMAT.format(timeRange.getEndTime()));
            return timeRange;
        }

        /**
         * 查找指定时间范围内的binlog位置
         */
        public void findPositionByTime(Date startTime, Date endTime, Set<String> databases, Set<String> tables) throws Exception {
            Map<String, Long> binlogFiles = getBinlogFiles();
            if (binlogFiles.isEmpty()) {
                logger.error("未找到可用的binlog文件");
                return;
            }

            logger.info("找到{}个binlog文件，使用顺序查找法定位位置", binlogFiles.size());
            boolean isFirst = true;
            FileTimeRange next = null;
            // 遍历每个binlog文件
            for (Map.Entry<String, Long> entry : binlogFiles.entrySet()) {
                String binlogFile = entry.getKey();
                long maxPosition = entry.getValue();
                logger.info("开始处理binlog文件: {}, 最大位置: {}", binlogFile, maxPosition);

                // 先获取文件的时间范围
                FileTimeRange fileTimeRange = getFileTimeRange(binlogFile, isFirst, next);
                next = fileTimeRange;
                isFirst = false;
                if (fileTimeRange == null) {
                    logger.warn("无法获取文件{}的时间范围，跳过该文件", binlogFile);
                    continue;
                }

                // 使用顺序查找定位时间范围内的位置
                List<PositionInfo> positionInfos = sequentialSearchPosition(fileTimeRange, startTime, endTime);
                PositionInfo startPos = null;
                PositionInfo endPos = null;
                if (positionInfos != null) {
                    for (PositionInfo positionInfo : positionInfos) {
                        if (positionInfo.isFound()) {
                            if (positionInfo.isStart()) {
                                startPos = positionInfo;
                            } else {
                                endPos = positionInfo;
                            }
                        }
                    }
                }
                if (startPos == null || endPos == null) {
                    continue;
                }

                // 如果找到了匹配的位置，输出结果并结束查找
                System.out.println("\n找到匹配的位置信息:");
                System.out.println("Binlog文件: " + startPos.getBinlogFile());
                System.out.println("位置: " + startPos.getPosition());
                System.out.println("事件时间: " + DATE_FORMAT.format(startPos.getEventTime()));
                System.out.println("\n可以使用以下命令进行数据回滚:");
                System.out.println("java -jar data-flush.jar -h " + host + " -P " + port +
                        " -u " + user + " -p <password> -f " + startPos.getBinlogFile() +
                        " -s " + startPos.getPosition());

                System.out.println("\n找到匹配的位置信息:");
                System.out.println("Binlog文件: " + endPos.getBinlogFile());
                System.out.println("位置: " + endPos.getPosition());
                System.out.println("事件时间: " + DATE_FORMAT.format(endPos.getEventTime()));
                System.out.println("\n可以使用以下命令进行数据回滚:");
                System.out.println("java -jar data-flush.jar -h " + host + " -P " + port +
                        " -u " + user + " -p <password> -f " + endPos.getBinlogFile() +
                        " -s " + endPos.getPosition());
                return; // 找到匹配位置后立即返回
            }

            logger.info("未找到匹配指定时间范围的binlog位置");
            System.out.println("未找到匹配指定时间范围的binlog位置");
        }

        /**
         * 使用顺序查找法定位指定时间范围内的binlog位置
         *
         * @param fileTimeRange binlog文件信息
         * @param startTime     开始时间
         * @param endTime       结束时间
         * @return 找到的位置信息，如果未找到则返回null
         */
        private List<PositionInfo> sequentialSearchPosition(FileTimeRange fileTimeRange, Date startTime, Date endTime) throws IOException, TimeoutException, InterruptedException {
            // 判断开始时间和结束时间 哪个需要被找到
            Date fileStartTime = fileTimeRange.getStartTime();
            Date fileEndTime = fileTimeRange.getEndTime();
            boolean needFindStartTime;
            boolean needFindEndTime;
            if (startTime.after(fileStartTime) && startTime.before(fileEndTime)) {
                // 开始时间在文件范围内，需要被找到
                needFindStartTime = true;
            } else {
                needFindStartTime = false;
            }
            if (endTime.after(fileStartTime) && endTime.before(fileEndTime)) {
                // 结束时间在文件范围内，需要被找到
                needFindEndTime = true;
            } else {
                needFindEndTime = false;
            }
            if (!needFindStartTime && !needFindEndTime) {
                return null;
            }
            BinaryLogClient client = new BinaryLogClient(host, port, user, password);
            client.setBinlogFilename(fileTimeRange.getFileName());
            client.setBinlogPosition(4);
            PositionInfo startPositionInfo = new PositionInfo();
            startPositionInfo.setStart(true);
            PositionInfo endPositionInfo = new PositionInfo();
            endPositionInfo.setStart(false);

            // 设置事件反序列化器
            EventDeserializer eventDeserializer = new EventDeserializer();
            eventDeserializer.setCompatibilityMode(
                    EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                    EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
            );
            client.setEventDeserializer(eventDeserializer);
            // 设置超时时间
            client.setHeartbeatInterval(1000);
            AtomicLong lastPosition = new AtomicLong(4);
            // 注册事件监听器，定位指定位置
            client.registerEventListener(event -> {
                EventHeaderV4 header = event.getHeader();
                long timestamp = header.getTimestamp();
                if (timestamp == 0) {
                    return;
                }
                if (needFindStartTime) {
                    if (timestamp >= startTime.getTime()) {
                        startPositionInfo.update(fileTimeRange.getFileName(), header.getPosition(), new Date(timestamp));
                    }
                }
                if (needFindEndTime) {
                    if (timestamp > endTime.getTime()) {
                        endPositionInfo.update(fileTimeRange.getFileName(), lastPosition.get(), endTime);
                    }
                }
                if (needFindStartTime) {
                    if (startPositionInfo.isFound()) {
                        if (needFindEndTime) {
                            if (endPositionInfo.isFound()) {
                                try {
                                    client.disconnect();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                }
                lastPosition.set(header.getPosition());
            });
            client.connect(300000);
            // 等待一小段时间，让事件处理完成
            while (client.isConnected()) {
                Thread.sleep(1000);
            }
            return Arrays.asList(startPositionInfo, endPositionInfo);
        }

        /**
         * 获取指定位置的事件时间信息
         */
        private EventTimeInfo getEventTimeAtPosition(String binlogFile, long position) {
            BinaryLogClient client = new BinaryLogClient(host, port, user, password);
            client.setBinlogFilename(binlogFile);
            client.setBinlogPosition(position);

            // 设置事件反序列化器
            EventDeserializer eventDeserializer = new EventDeserializer();
            eventDeserializer.setCompatibilityMode(
                    EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                    EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
            );
            client.setEventDeserializer(eventDeserializer);

            // 设置超时时间
            client.setHeartbeatInterval(1000);

            final EventTimeInfo timeInfo = new EventTimeInfo();

            // 注册事件监听器，只获取第一个事件的时间
            client.registerEventListener(event -> {
                if (!timeInfo.isFound()) {
                    EventHeaderV4 header = event.getHeader();
                    long timestamp = header.getTimestamp();
                    if (timestamp == 0) {
                        return;
                    }
                    long eventPosition = header.getPosition();
                    Date eventTime = new Date(timestamp);
                    logger.info("获取到事件时间: {}，位置: {}", DATE_FORMAT.format(eventTime), eventPosition);
                    timeInfo.update(eventTime, eventPosition);

//                     获取到时间信息后立即断开连接
                    try {
                        client.disconnect();
                    } catch (IOException e) {
                        logger.error("断开连接时发生错误", e);
                    }
                }
            });

            try {
                client.connect(5000); // 设置连接超时时间为5秒

                // 等待一小段时间，让事件处理完成
                Thread.sleep(1000);

                // 断开连接
                if (client.isConnected()) {
                    client.disconnect();
                }
            } catch (Exception e) {
                logger.error("获取事件时间信息时发生错误", e);
                try {
                    if (client.isConnected()) {
                        client.disconnect();
                    }
                } catch (IOException ex) {
                    logger.error("断开连接时发生错误", ex);
                }
                return null;
            }

            return timeInfo.isFound() ? timeInfo : null;
        }

        /**
         * 在指定位置附近查找满足所有条件的精确位置
         */
        private PositionInfo findExactPosition(String binlogFile, long position, Date startTime, Date endTime,
                                               Set<String> databases, Set<String> tables) {
            BinaryLogClient client = new BinaryLogClient(host, port, user, password);
            client.setBinlogFilename(binlogFile);
            client.setBinlogPosition(position);

            // 设置事件反序列化器
            EventDeserializer eventDeserializer = new EventDeserializer();
            eventDeserializer.setCompatibilityMode(
                    EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                    EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
            );
            client.setEventDeserializer(eventDeserializer);

            // 用于存储找到的位置信息
            final PositionInfo positionInfo = new PositionInfo();

            // 注册事件监听器
            client.registerEventListener(event -> {
                EventHeaderV4 header = event.getHeader();
                long timestamp = header.getTimestamp();
                long eventPosition = header.getNextPosition();
                Date eventTime = new Date(timestamp);

                // 检查事件是否在指定的时间范围内
                boolean matchesTime = true;
                if (startTime != null && eventTime.before(startTime)) {
                    matchesTime = false;
                }
                if (endTime != null && eventTime.after(endTime)) {
                    matchesTime = false;
                }

                // 如果事件在时间范围内，检查是否匹配指定的数据库和表
                if (matchesTime) {
                    EventData data = event.getData();
                    if (data instanceof TableMapEventData) {
                        TableMapEventData tableData = (TableMapEventData) data;
                        String database = tableData.getDatabase();
                        String table = tableData.getTable();

                        boolean matchesDatabase = databases.isEmpty() || databases.contains(database);
                        boolean matchesTable = tables.isEmpty() || tables.contains(table);

                        if (matchesDatabase && matchesTable) {
                            // 记录匹配的位置信息
                            positionInfo.update(binlogFile, eventPosition, eventTime);
                            logger.debug("找到匹配事件: 文件={}, 位置={}, 时间={}, 数据库={}, 表={}",
                                    binlogFile, eventPosition, DATE_FORMAT.format(eventTime), database, table);

                            // 找到匹配的位置后立即断开连接
                            try {
                                client.disconnect();
                            } catch (IOException e) {
                                logger.error("断开连接时发生错误", e);
                            }
                        }
                    }
                }
            });

            // 设置超时时间
            client.setHeartbeatInterval(2000);

            try {
                client.connect(5000); // 设置连接超时时间为5秒

                // 等待一段时间，让事件处理完成
                Thread.sleep(3000);

                // 断开连接
                if (client.isConnected()) {
                    client.disconnect();
                }
            } catch (Exception e) {
                logger.error("查找精确位置时发生错误", e);
                try {
                    if (client.isConnected()) {
                        client.disconnect();
                    }
                } catch (IOException ex) {
                    logger.error("断开连接时发生错误", ex);
                }
            }

            return positionInfo.isFound() ? positionInfo : null;
        }

        /**
         * 用于存储事件时间信息的内部类
         */
        private static class EventTimeInfo {
            private Date eventTime;
            private long position;
            private boolean found = false;

            public void update(Date eventTime, long position) {
                this.eventTime = eventTime;
                this.position = position;
                this.found = true;
            }

            public boolean isFound() {
                return found;
            }

            public Date getEventTime() {
                return eventTime;
            }

            public long getPosition() {
                return position;
            }
        }

        /**
         * 存储binlog文件的时间范围信息
         */
        private static class FileTimeRange {
            private String fileName;
            private Date startTime;
            private Date endTime;

            public String getFileName() {
                return fileName;
            }

            public void setFileName(String fileName) {
                this.fileName = fileName;
            }

            public Date getStartTime() {
                return startTime;
            }

            public void setStartTime(Date startTime) {
                this.startTime = startTime;
            }

            public Date getEndTime() {
                return endTime;
            }

            public void setEndTime(Date endTime) {
                this.endTime = endTime;
            }
        }

        /**
         * 用于存储位置信息的内部类
         */
        private static class PositionInfo {
            private Boolean isStart;
            private String binlogFile;
            private long position;
            private Date eventTime;
            private boolean found = false;

            public void update(String binlogFile, long position, Date eventTime) {
                this.binlogFile = binlogFile;
                this.position = position;
                this.eventTime = eventTime;
                this.found = true;
            }

            public boolean isStart() {
                return isStart;
            }

            public void setStart(Boolean start) {
                isStart = start;
            }

            public boolean isFound() {
                return found;
            }

            public String getBinlogFile() {
                return binlogFile;
            }

            public long getPosition() {
                return position;
            }

            public Date getEventTime() {
                return eventTime;
            }
        }
    }
}