package com.migration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runs different commands related to synchronization of database
 */
public class Runner {

    private static final String CONFIG_PROPERTIES_FILE = "synchronizer.properties";
    private static final String START_SYNC_LOG_COMMAND = "start-sync-log";
    private static final String STOP_SYNC_LOG_COMMAND = "stop-sync-log";
    private static final String DELETE_SYNC_LOG_COMMAND = "delete-sync-log";
    private static final String SYNC_PROCESS = "sync-process";

    private static final String SOURCE_DB_HOST = "source.db.host";
    private static final String SOURCE_DB_NAME = "source.db.name";
    private static final String SOURCE_DB_USER = "source.db.user";
    private static final String SOURCE_DB_PASSWORD = "source.db.password";
    private static final String TARGET_DB_HOST = "target.db.host";
    private static final String TARGET_DB_USER = "target.db.user";
    private static final String TARGET_DB_PASSWORD = "target.db.password";
    private static final String TARGET_DB_NAME = "target.db.name";
    private static final String SYNC_TABLES = "sync.tables";
    private static final String BATCH_SIZE = "batch.size";
    private static final String TASK_INTERVAL = "task.interval";

    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String COLUMN_TYPE = "COLUMN_TYPE";

    private static String sourceDatabaseHost;
    private static String sourceDatabaseName;
    private static String sourceDatabaseUser;
    private static String sourceDatabasePassword;
    private static String targetDatabaseHost;
    private static String targetDatabaseUser;
    private static String targetDatabasePassword;
    private static String targetDatabaseName;
    private static String batchSize;
    private static int taskInterval;
    private static String[] syncTables;

    private static Logger log = LogManager.getLogger(Runner.class);

    /**
     * Following commands are supported,
     * <p>
     * start-sync-log : Create Sync log tables and triggers
     * stop-sync-log : Drop the triggers
     * delete-sync-log : Drop the sync log tables
     * sync-process : Extract data from source DB and insert/update in the target DB.
     * <p>
     * Configs can be done using a properties file or flags passed as args.
     * Config list,
     * <p>
     * source.db.host
     * source.db.name
     * source.db.user
     * source.db.password
     * target.db.host
     * target.db.user
     * target.db.password
     * target.db.name
     * sync.tables
     * batch.size
     * task.interval
     *
     * @param args command and configuration flags
     */
    public static void main(String[] args) {

        if (0 == args.length) {
            log.error("Synchronizer requires a command to be passed. No command found");
            return;
        }

        if (1 == args.length) {

            log.info("No optional arguments passed. Reading from config file...");
            if (!getConfigFromFile()) {
                log.info("Error occurred while reading config file...");
                return;
            }
        } else {

            getConfigFromArgs(args);
        }

        // configs are available at this point. Progressing to execute the command
        switch (args[0]) {
            case START_SYNC_LOG_COMMAND:
                startSyncLog();
                break;
            case STOP_SYNC_LOG_COMMAND:
                stopSyncLog();
                break;
            case DELETE_SYNC_LOG_COMMAND:
                deleteSyncLog();
                break;
            case SYNC_PROCESS:
                startSyncProcess();
                break;
            default:
                log.error("Command does not match any of the expected commands, expected commands are, "
                        + START_SYNC_LOG_COMMAND + ", " + STOP_SYNC_LOG_COMMAND + ", " + DELETE_SYNC_LOG_COMMAND + ", "
                        + SYNC_PROCESS);
        }
    }

    private static void getConfigFromArgs(String[] args) {

        for (int i = 1; i < args.length; i++) {

            if ('-' == args[i].charAt(0) && args[i].length() > 2) {

                switch (args[i].substring(1)) {
                    case SOURCE_DB_HOST:
                        sourceDatabaseHost = args[i + 1];
                    case SOURCE_DB_NAME:
                        sourceDatabaseName = args[i + 1];
                        break;
                    case SOURCE_DB_USER:
                        sourceDatabaseUser = args[i + 1];
                        break;
                    case SOURCE_DB_PASSWORD:
                        sourceDatabasePassword = args[i + 1];
                        break;
                    case TARGET_DB_HOST:
                        targetDatabaseHost = args[i + 1];
                        break;
                    case TARGET_DB_USER:
                        targetDatabaseUser = args[i + 1];
                        break;
                    case TARGET_DB_PASSWORD:
                        targetDatabasePassword = args[i + 1];
                        break;
                    case TARGET_DB_NAME:
                        targetDatabaseName = args[i + 1];
                        break;
                    case BATCH_SIZE:
                        batchSize = args[i + 1];
                        break;
                    case TASK_INTERVAL:
                        try {
                            taskInterval = Integer.parseInt(args[i + 1]);
                        } catch (NumberFormatException nfe) {
                            log.error("Task interval should be an integer number, Erroneous config :" +
                                    args[i + 1]);
                        }
                        break;
                    case SYNC_TABLES:
                        syncTables = args[i + 1].split(",");
                        break;
                    default:
                        log.error("Config " + args[i] + " does not match any of the expected configs.");
                }
                i++;
            } else {
                log.error("Not a valid argument or option. Ignoring " + args[i]);
            }
        }
    }

    private static boolean getConfigFromFile() {

        try (BufferedReader br = new BufferedReader(new FileReader(CONFIG_PROPERTIES_FILE))) {

            String currentLine;
            while ((currentLine = br.readLine()) != null) {

                if (!currentLine.startsWith("#") && !currentLine.trim().equals("")) {

                    String[] configLine = currentLine.trim().split("=");

                    if (2 != configLine.length) {
                        log.error("Config [" + currentLine + "] does not follow correct format");
                        continue;
                    }

                    String config = configLine[0];
                    String value = configLine[1];

                    switch (config) {
                        case SOURCE_DB_HOST:
                            sourceDatabaseHost = value;
                            break;
                        case SOURCE_DB_NAME:
                            sourceDatabaseName = value;
                            break;
                        case SOURCE_DB_USER:
                            sourceDatabaseUser = value;
                            break;
                        case SOURCE_DB_PASSWORD:
                            sourceDatabasePassword = value;
                            break;
                        case TARGET_DB_HOST:
                            targetDatabaseHost = value;
                            break;
                        case TARGET_DB_USER:
                            targetDatabaseUser = value;
                            break;
                        case TARGET_DB_PASSWORD:
                            targetDatabasePassword = value;
                            break;
                        case TARGET_DB_NAME:
                            targetDatabaseName = value;
                            break;
                        case BATCH_SIZE:
                            batchSize = value;
                            break;
                        case TASK_INTERVAL:
                            try {
                                taskInterval = Integer.parseInt(value);
                            } catch (NumberFormatException nfe) {
                                log.error("Task interval should be an integer number, Erroneous config : " +
                                        value);
                            }
                            break;
                        case SYNC_TABLES:
                            syncTables = value.split(",");
                            break;
                        default:
                            log.error("Config " + config + " does not match any of the expected configs.");
                    }
                }
            }

        } catch (IOException e) {
            log.error("Error occurred while reading the config file", e);
            return false;
        }
        return true;
    }

    private static void startSyncLog() {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        String query;

        try (Connection dbConnection = getSourceDBConnection()) {

            if (null == dbConnection) {
                log.error("Could not make the database connection");
                return;
            }

            for (String table : syncTables) {

                String sourceTable = sourceDatabaseName + "." + table;

                statement = dbConnection.createStatement();
                query = "SELECT COLUMN_NAME,COLUMN_TYPE FROM information_schema.COLUMNS WHERE "
                        + "COLUMN_KEY ='PRI' AND TABLE_SCHEMA = '" + sourceDatabaseName
                        + "' AND TABLE_NAME = '" + table + "' LIMIT 1";
                resultSet = statement.executeQuery(query);
                resultSet.next();
                log.info(String.format("Query: [%s] ", query));

                String primeryCol = resultSet.getString(COLUMN_NAME);
                String primeryColType = resultSet.getString(COLUMN_TYPE);

                query = "DROP TABLE IF EXISTS " + sourceTable + "_SYNC;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Query: [%s] ", query));

                query = "CREATE TABLE " + sourceTable + "_SYNC ( SYNC_ID INT NOT NULL AUTO_INCREMENT," +
                        " " + primeryCol + " " + primeryColType + " NOT NULL, PRIMARY KEY (SYNC_ID)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Query: [%s] ", query));

                query = "DROP TRIGGER IF EXISTS " + table + "_SYNC_INSERT_TRIGGER;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Query: [%s] ", query));

                query = "DROP TRIGGER IF EXISTS " + table + "_SYNC_UPDATE_TRIGGER;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Query: [%s] ", query));

                query = "CREATE TRIGGER " + table + "_SYNC_INSERT_TRIGGER BEFORE INSERT " +
                        "ON " + sourceTable + " FOR EACH ROW BEGIN INSERT " +
                        "INTO " +
                        sourceTable + "_SYNC(" + primeryCol + ") " +
                        "VALUES(NEW." + primeryCol + "); " +
                        "END;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Query: [%s] ", query));

                query = "CREATE TRIGGER " + table + "_SYNC_UPDATE_TRIGGER BEFORE UPDATE " +
                        "ON " + sourceTable + " FOR EACH ROW BEGIN INSERT " +
                        "INTO " +
                        sourceTable + "_SYNC(" + primeryCol + ") " +
                        "VALUES(NEW." + primeryCol + "); " +
                        "END;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Query: [%s] ", query));
            }
        } catch (SQLException e) {

            log.error("Error occurred while executing SQL", e);
        } finally {

            if (null != preparedStatement) {
                try {
                    preparedStatement.close();
                } catch (SQLException ignored) {
                }
            }
            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (SQLException ignored) {
                }
            }
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private static void startSyncProcess() {

        Connection targetDBConnection = getTargetDBConnection();
        Connection sourceDBConnection = getSourceDBConnection();

        String query = null;

        for (String table : syncTables) {
            try {
                String targetTable = targetDatabaseName + "." + table;

                query = "CREATE TABLE IF NOT EXISTS " + targetTable + "_SYNC_VERSION (" +
                        " SYNC_ID INT) ENGINE=InnoDB DEFAULT CHARSET=latin1;";
                try (PreparedStatement preparedStatement = targetDBConnection.prepareStatement(query)) {
                    preparedStatement.execute();
                    log.info(String.format("Query: Create table for sync version at target database: [%s] ", query));

                }
                query = "INSERT INTO " + targetTable + "_SYNC_VERSION (SYNC_ID) SELECT 0 FROM DUAL WHERE NOT EXISTS (SELECT * FROM "
                        + targetTable + "_SYNC_VERSION);";
                try (PreparedStatement preparedStatement = targetDBConnection.prepareStatement(query)) {
                    preparedStatement.execute();
                    log.info(String.format("Query: Insert 0 if table is empty: [%s] ", query));
                }
            } catch (SQLException e) {
                log.error(String.format("Error occurred while executing SQL: Query : [%s] ", query), e);
            }
        }

        try {
            syncTables(syncTables, taskInterval, targetDBConnection, sourceDBConnection);
        } catch (InterruptedException e) {
            log.error("Error occurred while running sync task", e);
        }
    }


    @SuppressWarnings("InfiniteLoopStatement")
    private static void syncTables(String[] tables, int taskInterval, Connection targetDBConnection,
                                   Connection sourceDBConnection) throws InterruptedException {

        ConcurrentHashMap<String, String> primaryColMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, PreparedStatement> targetSyncVersionPsMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, PreparedStatement> dataInformationPsMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, PreparedStatement> dataExtractionPsMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, PreparedStatement> dataUpdatePsMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, PreparedStatement> targetVersionUpdatePsMap = new ConcurrentHashMap<>();

        for (String table : tables) {

            String query = "SELECT COLUMN_NAME FROM " +
                    "information_schema.COLUMNS WHERE COLUMN_KEY ='PRI' AND TABLE_SCHEMA = '"
                    + sourceDatabaseName + "' AND TABLE_NAME = '" + table + "' LIMIT 1;";
            try (ResultSet resultSet = targetDBConnection.prepareStatement(query).executeQuery()) {

                resultSet.next();
                primaryColMap.put(table, resultSet.getString(COLUMN_NAME));

            } catch (SQLException e) {
                log.error("Error occurred while executing primary column query", e);
            }

            try {
                query = "SELECT SYNC_ID FROM " + targetDatabaseName + "." + table + "_SYNC_VERSION;";
                targetSyncVersionPsMap.put(table, targetDBConnection.prepareStatement(query));

            } catch (SQLException e) {
                log.error(String.format("Error occurred while creating target db version prepared statement, Table [%s]", table), e);
            }
        }

        while (true) {

            if (log.isDebugEnabled()) {
                log.debug("Running sync task...");
            }

            boolean activateWait = true;
            for (String table : tables) {

                long startTime = System.currentTimeMillis();

                String primaryCol = primaryColMap.get(table);

                try {
                    String sourceTable = sourceDatabaseName + "." + table;
                    String targetTable = targetDatabaseName + "." + table;

                    int targetDBSyncVersion = 0;
                    int endingSyncId = 0;

                    try (ResultSet resultSet = targetSyncVersionPsMap.get(table).executeQuery()) {

                        if (resultSet.next()) {

                            targetDBSyncVersion = resultSet.getInt("SYNC_ID");
                            if (resultSet.wasNull()) {

                                log.error(String.format("Sync version returned from target is null. Data sync avoided " +
                                        "for this cycle. Table [%s] ", table));
                                continue;
                            }
                        }
                    }

                    ArrayList<String> updatingKeys = new ArrayList<>();

                    if (!dataInformationPsMap.contains(table)) {
                        String query = "SELECT MAX(SYNC_ID) FROM (" +
                                "SELECT SYNC_ID FROM " + sourceTable + "_SYNC WHERE SYNC_ID > ? limit " + batchSize + ") AS T;";
                        dataInformationPsMap.put(table, sourceDBConnection.prepareStatement(query));
                    }
                    dataInformationPsMap.get(table).setInt(1, targetDBSyncVersion);

                    try (ResultSet resultSet = dataInformationPsMap.get(table).executeQuery()) {

                        if (resultSet.next()) {

                            endingSyncId = resultSet.getInt("MAX(SYNC_ID)");
                        }
                    }
                    if (!dataExtractionPsMap.contains(table)) {

                        String query = "SELECT * FROM " + sourceTable + " WHERE " + primaryCol + " IN ( SELECT * FROM (SELECT DISTINCT "
                                + primaryCol + " FROM " + sourceTable + "_SYNC WHERE SYNC_ID > ? AND SYNC_ID <= ? )AS T);";

                        dataExtractionPsMap.put(table, sourceDBConnection.prepareStatement(query));
                    }
                    dataExtractionPsMap.get(table).setInt(1, targetDBSyncVersion);
                    dataExtractionPsMap.get(table).setInt(2, endingSyncId);

                    boolean updateSuccess;

                    long t0Time = System.currentTimeMillis();

                    try (ResultSet resultSet = dataExtractionPsMap.get(table).executeQuery()) {

                        if (log.isDebugEnabled()) {
                            long t1Time = System.currentTimeMillis();
                            log.info(String.format("Table [%s], Elapsed time for data extraction [%s ms], Target sync version [%s]",
                                    table, t1Time - t0Time, targetDBSyncVersion));
                        }

                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        StringBuilder columnNames = new StringBuilder();
                        StringBuilder bindVariables = new StringBuilder();

                        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {

                            if (i > 1) {
                                columnNames.append(", ");
                                bindVariables.append(", ");
                            }

                            columnNames.append(resultSetMetaData.getColumnName(i));
                            bindVariables.append('?');
                        }

                        if (!dataUpdatePsMap.containsKey(table)) {

                            String query = "REPLACE INTO " + targetTable + " ("
                                    + columnNames
                                    + ") VALUES ("
                                    + bindVariables
                                    + ");";

                            dataUpdatePsMap.put(table, targetDBConnection.prepareStatement(query));
                        }
                        PreparedStatement dataUpdatePs = dataUpdatePsMap.get(table);

                        while (resultSet.next()) {
                            updatingKeys.add(resultSet.getString(primaryCol));

                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                dataUpdatePs.setObject(i, resultSet.getObject(resultSetMetaData.getColumnName(i)));
                            }
                            dataUpdatePs.addBatch();
                        }

                        if (endingSyncId < targetDBSyncVersion) {

                            continue;
                        } else if (0 == endingSyncId) {

                            if (log.isDebugEnabled())
                                log.debug(String.format("No data to synchronize for table [%s]", table));
                            continue;
                        }

                        long t2Time = System.currentTimeMillis();

                        int[] updateResults = dataUpdatePs.executeBatch();

                        if (log.isDebugEnabled()) {
                            long t3Time = System.currentTimeMillis();
                            log.info(String.format("Table [%s], Elapsed time for data update [%s ms], Target sync version [%s]",
                                    table, t3Time - t2Time, targetDBSyncVersion));
                        }

                        updateSuccess = determineUpdateResults(updateResults, table);

                        long endTime = System.currentTimeMillis();
                        log.info(String.format("Table [%s], Elapsed time [%s ms], Target sync version [%s]",
                                table, endTime - startTime, targetDBSyncVersion));

                        if (log.isDebugEnabled())
                            log.debug(String.format("Table [%s], Sync'ed primary keys [%s]",
                                    table, String.join(", ", updatingKeys)));

                        if (updateResults.length > 1) {

                            activateWait = false;
                        }
                    }

                    if (updateSuccess) {
                        if (!targetVersionUpdatePsMap.contains(table)) {

                            String query = "UPDATE " + targetTable + "_SYNC_VERSION SET SYNC_ID = " + endingSyncId
                                    + " WHERE SYNC_ID = ?;";
                            targetVersionUpdatePsMap.put(table, targetDBConnection.prepareStatement(query));
                        }
                        targetVersionUpdatePsMap.get(table).setInt(1, targetDBSyncVersion);
                        targetVersionUpdatePsMap.get(table).execute();

                    } else {
                        log.error(String.format("Update of the complete batch was not successful, avoiding target" +
                                " DB sync version update, Table [%s]", table));
                    }

                } catch (SQLException e) {
                    if (e.getMessage().contains("Cannot add or update a child row: a foreign key constraint fails")) {
                        log.warn("Foreign key constraint error occurred. Will be fixed in next round : " + e.getMessage());
                    } else {
                        log.error(String.format("Error occurred while running SQL, Table [%s]", table), e);
                    }
                }
            }
            if (activateWait)
                Thread.sleep(taskInterval);
        }
    }

    private static boolean determineUpdateResults(int[] updateResults, String table) {

        ArrayList<String> failedUpdates = new ArrayList<>();
        boolean updateSuccess = true;

        for (int updateResult : updateResults) {
            if (updateResult == Statement.EXECUTE_FAILED) {

                updateSuccess = false;
                failedUpdates.add(String.valueOf(updateResult));
            }
        }
        if (updateSuccess) {

            if (log.isDebugEnabled())
                log.debug("Batch update is successful for all the entries");
        } else {
            log.error("Batch update is failed for some entries");
        }

        if (failedUpdates.size() > 0) {
            log.error(String.format("Table [%s], Indexes of failed updates: [%s] ", table, String.join(", ",
                    failedUpdates)));
            updateSuccess = false;
        }

        return updateSuccess;
    }

    private static void deleteSyncLog() {

        PreparedStatement preparedStatement = null;

        try (Connection dbConnection = getSourceDBConnection()) {

            if (null == dbConnection) {
                log.error("Could not make the database connection");
                return;
            }

            for (String table : syncTables) {

                String targetTable = targetDatabaseName + "." + table;

                preparedStatement = dbConnection.prepareStatement("DROP TRIGGER IF EXISTS "
                        + targetTable + "_SYNC_INSERT_TRIGGER");
                preparedStatement.execute();

                preparedStatement = dbConnection.prepareStatement("DROP TRIGGER IF EXISTS "
                        + targetTable + "_SYNC_UPDATE_TRIGGER");
                preparedStatement.execute();

                preparedStatement = dbConnection.prepareStatement("DROP TABLE IF EXISTS "
                        + targetTable + "_SYNC");
                preparedStatement.execute();


            }
        } catch (SQLException e) {

            log.error("Error occurred while executing SQL", e);
        } finally {

            if (null != preparedStatement) {
                try {
                    preparedStatement.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private static void stopSyncLog() {

        PreparedStatement preparedStatement = null;

        try (Connection dbConnection = getSourceDBConnection()) {

            if (null == dbConnection) {
                log.error("Could not make the database connection");
                return;
            }

            for (String table : syncTables) {

                String targetTable = targetDatabaseName + "." + table;

                preparedStatement = dbConnection.prepareStatement("DROP TRIGGER IF EXISTS "
                        + targetTable + "_SYNC_INSERT_TRIGGER");
                preparedStatement.execute();

                preparedStatement = dbConnection.prepareStatement("DROP TRIGGER IF EXISTS "
                        + targetTable + "_SYNC_UPDATE_TRIGGER");
                preparedStatement.execute();
            }
        } catch (SQLException e) {

            log.error("Error occurred while executing SQL", e);
        } finally {

            if (null != preparedStatement) {
                try {
                    preparedStatement.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private static Connection getSourceDBConnection() {

        Connection dbConnection = null;
        try {
            dbConnection = DriverManager.getConnection("jdbc:mysql://" + sourceDatabaseHost + "/"
                    + sourceDatabaseName + "?user=" + sourceDatabaseUser + "&password=" + sourceDatabasePassword
                    + "&useSSL=false");
        } catch (SQLException e) {
            log.error("Error occurred while creating source database connection", e);
        }

        return dbConnection;
    }

    private static Connection getTargetDBConnection() {

        Connection dbConnection = null;
        try {
            dbConnection = DriverManager.getConnection("jdbc:mysql://" + targetDatabaseHost + "/"
                    + targetDatabaseName + "?user=" + targetDatabaseUser + "&password=" + targetDatabasePassword
                    + "&useSSL=false");
        } catch (SQLException e) {
            log.error("Error occurred while creating target database connection", e);
        }

        return dbConnection;
    }
}
