import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class Runner {

    private static final FileRender fr = new FileRender();
    private static final Runner m = new Runner();
    private PreparedStatement preparedStatement;
    private Statement statement;
    private ResultSet resultSet;
    private String SourceDBUser;
    private String SourceDBPass;
    private String TargetDBUser;
    private String TargetDBPass;

    private static Logger log = LogManager.getLogger(Runner.class);


    public static void main(String[] args) throws Exception {

        System.out.println("####################################################");
        System.out.println("Welcome to Realtime Database Synchronization Client");
        System.out.println("####################################################\n\n");


        Scanner scanner = new Scanner(System.in);

        System.out.println("Available Operations.\n");

        System.out.println("1. Create Triggers in Source Database");
        System.out.println("2. Start Synchronization");

        System.out.print("\nPlease enter the opreation number :");
        String input = scanner.nextLine();


        fr.ReadSyncTablePropertise();


        if (input.equals("1")) {
            m.getSourceDBCredincials(scanner);
            m.createTrggers();
        } else if (input.equals("2")) {

            m.getSourceDBCredincials(scanner);
            m.getTargetDBCredincials(scanner);

            System.out.print("\nNothing to do here :) ");
        } else {
            System.out.print("\nInvalide Option ! ");
        }
    }

    public void createTrggers() throws Exception {


        DBConnection db1 = new DBConnection();
        db1.connect("jdbc:mysql://" + fr.sourceDbHost + "/" + fr.sourceSchema + "?user=" + m.SourceDBUser + "&password=" + m.SourceDBPass + "&useSSL=false");

        //String schema = "IS_CARBON";

        for (String table : fr.syncTables) {
            try {


                statement = db1.connection.createStatement();

                resultSet = statement.executeQuery("SELECT COLUMN_NAME,COLUMN_TYPE FROM information_schema.COLUMNS WHERE COLUMN_KEY ='PRI' AND TABLE_SCHEMA = '" + fr.sourceSchema + "' AND TABLE_NAME = '" + table + "' LIMIT 1");
                resultSet.next();

                String primeryCol = resultSet.getString(information_schema.COLUMN_NAME.toString());
                String primeryColType = resultSet.getString(information_schema.COLUMN_TYPE.toString());

                preparedStatement = db1.connection.prepareStatement("DROP TABLE IF EXISTS " + fr.sourceSchema + "." + table + "_SYNC;");
                preparedStatement.execute();

                preparedStatement = db1.connection.prepareStatement("DROP TABLE IF EXISTS " + fr.sourceSchema + "." + table + "_SYNC;");
                preparedStatement.execute();


                preparedStatement = db1.connection.prepareStatement("CREATE TABLE " + fr.sourceSchema + "." + table + "_SYNC (" +
                        " SYC_ID INT NOT NULL AUTO_INCREMENT," +
                        " " + primeryCol + " " + primeryColType + " NOT NULL," +
                        " PRIMARY KEY (SYC_ID)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;");
                preparedStatement.execute();


                preparedStatement = db1.connection.prepareStatement(" DROP TRIGGER IF EXISTS " + table + "_SYNC_INSERT_TRIGR;");
                preparedStatement.execute();

                preparedStatement = db1.connection.prepareStatement(" DROP TRIGGER IF EXISTS " + table + "_SYNC_UPDATE_TRIGR;");
                preparedStatement.execute();

                preparedStatement = db1.connection.prepareStatement("CREATE " +
                        "TRIGGER " + table + "_SYNC_INSERT_TRIGR BEFORE INSERT " +
                        "ON " +
                        "" + fr.sourceSchema + "." + table + " FOR EACH ROW BEGIN INSERT " +
                        "INTO " +
                        "" + fr.sourceSchema + "." + table + "_SYNC(" + primeryCol + ") " +
                        "VALUES(NEW." + primeryCol + "); " +
                        "END;");
                preparedStatement.execute();


                preparedStatement = db1.connection.prepareStatement("CREATE " +
                        "TRIGGER " + table + "_SYNC_UPDATE_TRIGR BEFORE UPDATE " +
                        "ON " +
                        "" + fr.sourceSchema + "." + table + " FOR EACH ROW BEGIN UPDATE " +
                        "INTO " +
                        "" + fr.sourceSchema + "." + table + "_SYNC(" + primeryCol + ") " +
                        "VALUES(NEW." + primeryCol + "); " +
                        "END;");
                preparedStatement.execute();


            } catch (Exception e) {


            }


        }
    }

    public void getSourceDBCredincials(Scanner scanner) {

        System.out.print("Please Enter username for SOURCE_DB :");
        m.SourceDBUser = scanner.nextLine();

        System.out.print("Please Enter Password for SOURCE_DB :");
        m.SourceDBPass = scanner.nextLine();
    }

    public void getTargetDBCredincials(Scanner scanner) {

        System.out.print("Please Enter username for TARGET_DB :");
        m.TargetDBUser = scanner.nextLine();

        System.out.print("Please Enter Password for TARGET_DB :");
        m.TargetDBPass = scanner.nextLine();
    }

    public void startSync() {

        DBConnection db1 = new DBConnection();
        db1.connect("jdbc:mysql://" + fr.sourceDbHost + "/" + fr.sourceSchema + "?user=" + m.SourceDBUser + "&password=" + m.SourceDBPass + "&useSSL=false");

        //statement = db1.connection.createStatement();

        DBConnection db2 = new DBConnection();
        db2.connect("jdbc:mysql://" + fr.sourceDbHost + "/" + fr.sourceSchema + "?user=" + m.TargetDBUser + "&password=" + m.TargetDBPass + "&useSSL=false");

        for (String table : fr.syncTables) {
            try {

//                preparedStatement = db2.connection.prepareStatement("DROP TABLE IF EXISTS " + fr.targetSchema + "." + table + "_SYNCD_ID;");
//                preparedStatement.execute();

                preparedStatement = db1.connection.prepareStatement("CREATE TABLE IF NOT EXISTS" + fr.targetSchema + "." + table + "_SYNCD_ID (" +
                        " SYCD_ID INT" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;");
                preparedStatement.execute();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        while (true) {
            for (String table : fr.syncTables) {
                try {

                    resultSet = db2.connection.createStatement().executeQuery("SELECT SYCD_ID FROM" + fr.targetSchema + "." + table + "_SYNCD_ID");
                    resultSet.next();
                    String SYCD_ID = resultSet.getString(syncd_id.SYCD_ID.toString());

                    resultSet = db1.connection.createStatement().executeQuery("SELECT SYCD_ID FROM" + fr.targetSchema + "." + table + "_SYNCD_ID");
                    resultSet.next();
                    // String SYCD_ID = resultSet.getString(syncd_id.SYCD_ID.toString());

                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    enum information_schema {
        COLUMN_NAME, COLUMN_TYPE;
    }

    enum syncd_id {
        SYCD_ID;
    }

//    public void getPrimaryKey(){
//
//        statement = db1.connection.createStatement();
//
//        resultSet = statement.executeQuery("SELECT COLUMN_NAME,COLUMN_TYPE FROM information_schema.COLUMNS WHERE COLUMN_KEY ='PRI' AND TABLE_SCHEMA = '"+fr.sourceSchema+"' AND TABLE_NAME = '" + table + "' LIMIT 1");
//        resultSet.next();
//
//        String primeryCol = resultSet.getString(information_schema.COLUMN_NAME.toString());
//        String primeryColType = resultSet.getString(information_schema.COLUMN_TYPE.toString());
//
//    }


}
