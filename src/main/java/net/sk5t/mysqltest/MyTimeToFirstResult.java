package net.sk5t.mysqltest;

import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

public class MyTimeToFirstResult {

    private static final Logger logger = LoggerFactory.getLogger(MyTimeToFirstResult.class);

    private final Jdbi db;
    private long sttm;

    public MyTimeToFirstResult(Jdbi db) {
        this.db = db;
    }

    public BigDecimal reduceBigResultSet(int fetchSize, Duration maxTime) throws SQLException {
        sttm = System.currentTimeMillis();
        final Instant deadline = Instant.now().plus(maxTime);
        return db.withHandle(handle -> handle.createQuery("select foo, quux, burzum from stuff")
                .setFetchSize(fetchSize)
                .reduceResultSet(BigDecimal.ZERO,
                        (previous, rs, ctx) -> {
                            if (Instant.now().isAfter(deadline)) {
                                throw new RuntimeException("hit deadline from " + maxTime);
                            }
                            if (previous.equals(BigDecimal.ZERO)) {
                                long elap = System.currentTimeMillis() - sttm;
                                logger.info("first row at elapsedMs = {}", elap);
                            }
                            return previous.add(BigDecimal.valueOf(rs.getDouble(1)));
                        }));
    }
}
