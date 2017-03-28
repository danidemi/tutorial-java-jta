package com.danidemi;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.Configuration;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.resource.jdbc.PoolingDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.UUID;

import static java.lang.String.format;

/** Inspired by https://www.codeproject.com/Articles/143432/An-Introduction-to-Bitronix-JTA-Transaction-Manage */
public class BTMWithH2Programmatically {

    private static final String USER_NAME = "javauser";
    private static final String PASSWORD = "javadude";

    private static final String INSERT_QUERY = "insert into student(roll_number,name,class,section) values (?,?,?,?)";
    private static final String SELECT_QUERY = "select * from student";

    private static final Logger log = LoggerFactory.getLogger(BTMWithH2Programmatically.class);

    public static void main(String[] args) {
        new BTMWithH2Programmatically().run();
    }

    private void run() {

//        JdbcDataSource ds = new JdbcDataSource();
//        ds.setURL( format("jdbc:h2:%s/test", FileUtils.getTempDirectory().getAbsolutePath()) );
//        ds.setUser("sa");
//        ds.setPassword("sa");




        Configuration conf = TransactionManagerServices.getConfiguration();
        conf.setServerId(UUID.randomUUID().toString());
        conf.setResourceConfigurationFilename( getClass().getResource("/btm.properties").getFile() );

        BitronixTransactionManager btm = TransactionManagerServices.getTransactionManager();

        PoolingDataSource ds = new PoolingDataSource();
        ds.setUniqueName("code");
        ds.setMaxPoolSize(10);
        ds.setMinPoolSize(2);
        ds.setClassName("org.h2.jdbcx.JdbcDataSource");
        Properties driverProperties = new Properties();
        driverProperties.setProperty("URL", format("jdbc:h2:%s\\test", FileUtils.getTempDirectory().getAbsolutePath()));
        driverProperties.setProperty("user", "sa");
        driverProperties.setProperty("password", "sa");
        //driverProperties.setProperty("URL", format("jdbc:h2:%s\\test;USER=sa;PASSWORD=sa", FileUtils.getTempDirectory().getAbsolutePath()));
        ds.setDriverProperties(driverProperties);
        ds.init();


        try {
            btm.begin();
            Connection createConnection = ds.getConnection();
            createConnection.createStatement().execute(IOUtils.toString( BTMWithH2Programmatically.class.getResourceAsStream("/create_tables.sql") ));
            createConnection.close();
            btm.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // a successful Bitronix transaction
        try		{

            btm.begin();
            Connection connection = ds.getConnection();

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
                Connection connection = ds.getConnection();
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
                Connection connection = ds.getConnection();
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
            Connection connection = ds.getConnection();
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