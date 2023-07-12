package org.car;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {
    private String dbURL = "jdbc:mysql://localhost:3306/rental_car";
    private String user = "root";

    private String pass = "pass";

    public Connection getConn() throws SQLException {
        return DriverManager.getConnection(dbURL, user, pass);
    }

}
