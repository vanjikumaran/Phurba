import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DBConnection {

    private static Logger log = LogManager.getLogger(DBConnection.class);
    Connection connection;

    void connect(String jdbcURL) throws SQLException {

        try {
            Class.forName("com.mysql.jdbc.Driver");

            connection = DriverManager.getConnection(jdbcURL);
        } catch (ClassNotFoundException e) {
            log.error("Error occurred while executing SQL");
        }
    }
}
