package net.sk5t.mysqltest;

import com.google.common.base.Stopwatch;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Random;

public class MyTimeToFirstResultTest {

    static final Logger logger = LoggerFactory.getLogger(MyTimeToFirstResultTest.class);

    final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.28");

    /**
     * Configure the DataSource - just a "supplier of Connection", nothing fancy.
     * <p>
     * <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html">docs</a>
     *
     * @param clientCursor use client cursor option
     * @param readahead    use readAhead option
     * @return DataSource
     * @throws SQLException boom
     */
    DataSource makeDatasource(boolean clientCursor, boolean readahead) throws SQLException {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setUrl(mysql.getJdbcUrl());
        // ds.setUser(mysql.getUsername());
        // ds.setPassword(mysql.getPassword());
        ds.setAllowLoadLocalInfile(true);
        ds.setUseCursorFetch(clientCursor);
        ds.setUseReadAheadInput(readahead);
        ds.setUser("root");
        ds.setPassword("test");
        return ds;
    }

    /**
     * Start the container and fill it with ~200MB test data
     * @throws SQLException
     * @throws IOException
     */
    @BeforeClass
    public void setUp() throws SQLException, IOException {

        logger.info("starting {}", mysql);
        mysql.addEnv("MYSQL_LOCAL_INFILE", "1");
        mysql.start();
        logger.info("started mysql");

        Stopwatch sw = Stopwatch.createStarted();
        makeTestData(Jdbi.create(makeDatasource(false, false)), 2_999_000L);
        logger.info("loaded test data in {}", sw);
    }

    void makeTestData(Jdbi db, long count) throws IOException {

        db.useTransaction(handle -> handle.execute("SET GLOBAL local_infile=1"));
        db.useTransaction(handle -> {
            handle.execute("create table stuff (foo decimal(8, 6), quux varchar(90), burzum varchar(90))");
        });
        var rnd = new Random();
        var f = File.createTempFile("foo", "csv");
        try (var bw = new BufferedWriter(new FileWriter(f))) {
            for (int i = 0; i < count; i++) {
                bw.write("" + rnd.nextDouble() + "\tasjdfkladsjflasjdkfdsjkflasd\thjsadkfhjkhjksfdhjkfadshjsfdhj");
                bw.newLine();
            }
        }
        logger.info("generated file {} of {} MiB; loading", f, f.length() / (1024 * 1024));

        db.useTransaction(handle -> {
            handle.execute(" LOAD DATA LOCAL INFILE '" + f.getAbsolutePath() + "' into table stuff FIELDS TERMINATED BY '\\t'");
        });

        logger.info("makeTestData completed");
    }


    @AfterClass
    public void cleanup() {
        mysql.stop();
    }

    @DataProvider(name = "pageSize")
    Object[][] pageSizes() {
        return new Object[][]{
                {Integer.MIN_VALUE, false},
                {0, false},
                {10, false},
                {500, false},
                {Integer.MAX_VALUE, false},
                {Integer.MIN_VALUE, true},
                {0, true},
                {10, true},
                {500, true}
        };
    }

    @Test(dataProvider = "pageSize")
    public void testReduceBigResultSet(int pageSize, boolean useCursor) throws SQLException {

        var jdbi = Jdbi.create(makeDatasource(useCursor, true));
        var sut = new MyTimeToFirstResult(jdbi);

        logger.info("========= page size {}; client cursor = {}", pageSize, useCursor);
        System.gc();
        var sw = Stopwatch.createStarted();
        try {
            var answer = sut.reduceBigResultSet(pageSize, Duration.ofMinutes(1));
            logger.info("reduced to {} in {}", answer, sw);
        } catch (Exception e) {
            logger.info("ex: {}", e.getMessage());
        }
        logger.info("max mem {}, free mem {}",
                Runtime.getRuntime().maxMemory(), Runtime.getRuntime().freeMemory());

    }
}
