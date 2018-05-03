package com.migration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

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
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String COLUMN_TYPE = "COLUMN_TYPE";
    private static final String SYNC_VERSION = "SYNC_VERSION";

    private static String sourceDatabaseHost;
    private static String sourceDatabaseName;
    private static String sourceDatabaseUser;
    private static String sourceDatabasePassword;
    private static String targetDatabaseHost;
    private static String targetDatabaseUser;
    private static String targetDatabasePassword;
    private static String targetDatabaseName;
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

                switch (args[i]) {
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

                statement = dbConnection.createStatement();
                query = "SELECT COLUMN_NAME,COLUMN_TYPE FROM information_schema.COLUMNS WHERE "
                        + "COLUMN_KEY ='PRI' AND TABLE_SCHEMA = '" + sourceDatabaseName
                        + "' AND TABLE_NAME = '" + table + "' LIMIT 1";
                resultSet = statement.executeQuery(query);
                resultSet.next();
                log.info(String.format("Successfully executed query [%s] ", query));

                String primeryCol = resultSet.getString(COLUMN_NAME);
                String primeryColType = resultSet.getString(COLUMN_TYPE);

                query = "DROP TABLE IF EXISTS " + sourceDatabaseName + "." + table + "_SYNC;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Successfully executed query [%s] ", query));

                query = "CREATE TABLE " + sourceDatabaseName + "." + table + "_SYNC ( SYNC_ID INT NOT NULL AUTO_INCREMENT," +
                        " " + primeryCol + " " + primeryColType + " NOT NULL, PRIMARY KEY (SYNC_ID)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Successfully executed query [%s] ", query));

                query = "DROP TRIGGER IF EXISTS " + table + "_SYNC_INSERT_TRIGGER;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Successfully executed query [%s] ", query));

                query = "DROP TRIGGER IF EXISTS " + table + "_SYNC_UPDATE_TRIGGER;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Successfully executed query [%s] ", query));

                query = "CREATE TRIGGER " + table + "_SYNC_INSERT_TRIGGER BEFORE INSERT " +
                        "ON " + sourceDatabaseName + "." + table + " FOR EACH ROW BEGIN INSERT " +
                        "INTO " +
                        sourceDatabaseName + "." + table + "_SYNC(" + primeryCol + ") " +
                        "VALUES(NEW." + primeryCol + "); " +
                        "END;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Successfully executed query [%s] ", query));

                query = "CREATE TRIGGER " + table + "_SYNC_UPDATE_TRIGGER BEFORE UPDATE " +
                        "ON " + sourceDatabaseName + "." + table + " FOR EACH ROW BEGIN INSERT " +
                        "INTO " +
                        sourceDatabaseName + "." + table + "_SYNC(" + primeryCol + ") " +
                        "VALUES(NEW." + primeryCol + "); " +
                        "END;";
                preparedStatement = dbConnection.prepareStatement(query);
                preparedStatement.execute();
                log.info(String.format("Successfully executed query [%s] ", query));
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

        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        ArrayList<Object> rowValues = new ArrayList<>();

        Connection targetDBConnection = getTargetDBConnection();
        Connection sourceDBConnection = getSourceDBConnection();

        for (String table : syncTables) {
            try {

                preparedStatement = targetDBConnection.prepareStatement("CREATE TABLE IF NOT EXISTS "
                        + targetDatabaseName + "." + table + "_SYNC_VERSION (" +
                        " SYNC_ID INT" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;");
                preparedStatement.execute();

            } catch (SQLException e) {
                log.error("Error occurred while executing SQL", e);
            }
        }

        int targetDBSyncId;
        int sourceDBMaxSyncId;
        int nextSyncId;
        String primeryKey;

        while (true) {
            for (String table : syncTables) {
                try {
                    targetDBSyncId = 0;
                    sourceDBMaxSyncId = 0;
                    nextSyncId = 0;


                    primeryKey = null;

                    resultSet = targetDBConnection.createStatement().executeQuery("SELECT SYNC_ID FROM "
                            + targetDatabaseName + "." + table + "_SYNC_VERSION;");

                    if (resultSet.next()) {
                        targetDBSyncId = resultSet.getInt("SYNC_ID");
                        if (resultSet.wasNull()) {
                            // handle NULL field value
                        }
                    }

                    resultSet = sourceDBConnection.createStatement().executeQuery("SELECT MAX(SYNC_ID) FROM "
                            + sourceDatabaseName + "." + table + "_SYNC;");

                    if (resultSet.next()) {
                        sourceDBMaxSyncId = resultSet.getInt("MAX(SYNC_ID)");
                        if (resultSet.wasNull()) {
                            // handle NULL field value
                        }
                    }

                    if (sourceDBMaxSyncId > targetDBSyncId) {

                        resultSet = sourceDBConnection.createStatement().executeQuery("SELECT SYNC_ID FROM "
                                + sourceDatabaseName + "." + table + "_SYNC WHERE SYNC_ID > " + targetDBSyncId
                                + " ORDER BY SYNC_ID LIMIT 1;");

                        if (resultSet.next()) {
                            nextSyncId = resultSet.getInt("SYNC_ID");
                            if (resultSet.wasNull()) {
                                // handle NULL field value
                            }
                        }

                        resultSet = sourceDBConnection.createStatement().executeQuery("SELECT COLUMN_NAME FROM " +
                                "information_schema.COLUMNS WHERE COLUMN_KEY ='PRI' AND TABLE_SCHEMA = '"
                                + sourceDatabaseName + "' AND TABLE_NAME = '" + table + "' LIMIT 1;");
                        resultSet.next();

                        String primeryCol = resultSet.getString(COLUMN_NAME);

                        resultSet = sourceDBConnection.createStatement().executeQuery("SELECT SYNC_ID,"
                                + primeryCol + " FROM " + sourceDatabaseName + "." + table + "_SYNC WHERE SYNC_ID = "
                                + nextSyncId + ";");

                        if (resultSet.next()) {
                            primeryKey = resultSet.getString(primeryCol);
                            if (resultSet.wasNull()) {
                                // handle NULL field value
                            }
                        }

                        resultSet = sourceDBConnection.createStatement().executeQuery("SELECT * FROM "
                                + sourceDatabaseName + "." + table + " WHERE " + primeryCol + " = '" + primeryKey + "';");
                        resultSet.next();

                        ResultSetMetaData meta = resultSet.getMetaData();
                        StringBuilder columnNames = new StringBuilder();
                        StringBuilder bindVariables = new StringBuilder();

                        for (int i = 1; i <= meta.getColumnCount(); i++) {

                            if (i > 1) {
                                columnNames.append(", ");
                                bindVariables.append(", ");
                            }

                            columnNames.append(meta.getColumnName(i));
                            bindVariables.append('?');
                            rowValues.add(resultSet.getObject(meta.getColumnName(i)));
                        }

                        String sql = "REPLACE INTO " + targetDatabaseName + "." + table + " ("
                                + columnNames
                                + ") VALUES ("
                                + bindVariables
                                + ");";

                        preparedStatement = targetDBConnection.prepareStatement(sql);

                        for (int i = 0; i < rowValues.size(); i++) {

                            preparedStatement.setObject(i + 1, rowValues.get(i));
                        }

                        preparedStatement.execute();

                        preparedStatement = targetDBConnection.prepareStatement("update " + targetDatabaseName
                                + "." + table + "_SYNC_VERSION SET SYNC_ID = " + nextSyncId + " WHERE SYNC_ID = "
                                + targetDBSyncId + ";");

                        preparedStatement.execute();

                        rowValues.clear();
                    }

                } catch (SQLException e) {
                    log.error("Error occurred while creating target database connection", e);
                } finally {

                    if (null != resultSet) {
                        try {
                            resultSet.close();
                        } catch (SQLException ignored) {
                        }
                    }
                }
            }
        }
    }

    private static void deleteSyncLog() {

        PreparedStatement preparedStatement = null;

        try (Connection dbConnection = getSourceDBConnection()) {

            if (null == dbConnection) {
                log.error("Could not make the database connection");
                return;
            }

            for (String table : syncTables) {

                preparedStatement = dbConnection.prepareStatement("DROP TRIGGER IF EXISTS "
                        + targetDatabaseName + "." + table + "_SYNC_INSERT_TRIGGER");
                preparedStatement.execute();

                preparedStatement = dbConnection.prepareStatement("DROP TRIGGER IF EXISTS "
                        + targetDatabaseName + "." + table + "_SYNC_UPDATE_TRIGGER");
                preparedStatement.execute();

                preparedStatement = dbConnection.prepareStatement("DROP TABLE IF EXISTS "
                        + targetDatabaseName + "." + table + "_SYNC");
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

                preparedStatement = dbConnection.prepareStatement("DROP TRIGGER IF EXISTS "
                        + targetDatabaseName + "." + table + "_SYNC_INSERT_TRIGGER");
                preparedStatement.execute();

                preparedStatement = dbConnection.prepareStatement("DROP TRIGGER IF EXISTS "
                        + targetDatabaseName + "." + table + "_SYNC_UPDATE_TRIGGER");
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
