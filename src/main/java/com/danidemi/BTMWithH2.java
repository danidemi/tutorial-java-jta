package com.danidemi;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static java.lang.String.format;

/** Inspired by https://www.codeproject.com/Articles/143432/An-Introduction-to-Bitronix-JTA-Transaction-Manage */
public class BTMWithH2 {

    private static final String USER_NAME = "javauser";
    private static final String PASSWORD = "javadude";

    private static final String INSERT_QUERY = "insert into student(roll_number,name,class,section) values (?,?,?,?)";
    private static final String SELECT_QUERY = "select * from student";

    private static final Logger log = LoggerFactory.getLogger(BTMWithH2.class);

    public static void main(String[] args) {
        new BTMWithH2().run();
    }

    private void run() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL( format("jdbc:h2:%s/test", FileUtils.getTempDirectory().getAbsolutePath()) );
        ds.setUser("sa");
        ds.setPassword("sa");

        BitronixTransactionManager btm = TransactionManagerServices.getTransactionManager();

        // a successful Bitronix transaction
        try		{

            Connection createConnection = ds.getConnection(USER_NAME, PASSWORD);
            createConnection.createStatement().execute(IOUtils.toString( BTMWithH2.class.getResourceAsStream("/create_tables.sql") ));
            createConnection.close();


            btm.begin();
            Connection connection = ds.getConnection(USER_NAME, PASSWORD);

            PreparedStatement pstmt =
                    connection.prepareStatement(INSERT_QUERY);
            for(int index = 1; index <= 5; index++) {
                pstmt.setInt(1,index);
                pstmt.setString(2, "student_" + index);
                pstmt.setString(3, "" + (4 + index));
                pstmt.setString(4, "A");
                pstmt.executeUpdate();
            }
            pstmt.close();

            connection.close();

            btm.commit();

        } catch (Exception ex) {
            ex.printStackTrace();
            try {
                btm.rollback();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // a failing Bitronix transaction
        try		{

            btm.begin();

            {
                log.info("Inserting student in section B");
                Connection connection = ds.getConnection(USER_NAME, PASSWORD);
                PreparedStatement pstmt =
                        connection.prepareStatement(INSERT_QUERY);
                for (int index = 1; index <= 5; index++) {
                    pstmt.setInt(1, index);
                    pstmt.setString(2, "student_" + index);
                    pstmt.setString(3, "" + (4 + index));
                    pstmt.setString(4, "B");
                    pstmt.executeUpdate();
                }
                pstmt.close();
                connection.close();
            }

            {
                log.info("Inserting student in section C");
                Connection connection = ds.getConnection(USER_NAME, PASSWORD);
                PreparedStatement pstmt =
                        connection.prepareStatement(INSERT_QUERY);
                for (int index = 1; index <= 5; index++) {
                    pstmt.setInt(1, index);
                    pstmt.setString(2, "student_" + index);
                    pstmt.setString(3, "" + (4 + index));
                    pstmt.setString(4, "C");
                    pstmt.executeUpdate();
                }
                pstmt.close();
                connection.close();
                if(.7 > .5) {
                    throw new IllegalStateException("Fake exception to test rolling back.");
                }
            }

            btm.commit();

        } catch (Exception ex) {
            log.warn("Error while executing SQL.", ex);
            try {
                btm.rollback();
            } catch (Exception e) {
                log.error("Error rolling back", e);
            }
        }

        // a successful select Bitronix transaction
        try {
            btm.begin();
            Connection connection = ds.getConnection(USER_NAME, PASSWORD);
            PreparedStatement pstmt = connection.prepareStatement(SELECT_QUERY);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                String student = "Student: " + rs.getInt("roll_number") + " " + rs.getString("name") + " " + rs.getString("class") + " " + rs.getString("section");
                log.info("Loaded from DB: {}.", student);
            }
            rs.close();
            pstmt.close();
            connection.close();
            btm.commit();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            try {
                btm.rollback();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //mySQLDS.close();
        btm.shutdown();
    }
}