/*
 *   Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package functionalTests;

import com.amazon.pocketEtl.extractor.SqlExtractor;
import com.google.common.collect.ImmutableMap;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SqlExtractorFunctionalTest {
    private static ComboPooledDataSource dataSource;
    private Connection connection;

    private final static String DROP_SQL = "DROP TABLE IF EXISTS test_data";

    private final static String CREATE_SQL =
            "CREATE TABLE test_data (" +
            " id INT NOT NULL," +
            " aString VARCHAR(20)," +
            " aNumber BIGINT," +
            " aDateTime TIMESTAMP," +
            " aBoolean BOOLEAN," +
            " PRIMARY KEY (id))";

    private final SqlExtractor<TestDTO2> sqlExtractor = SqlExtractor.of(dataSource, "SELECT * FROM test_data", TestDTO2.class);

    @BeforeClass
    public static void startDatabase() throws Exception {
        dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("org.hsqldb.jdbc.JDBCDriver");
        dataSource.setJdbcUrl("jdbc:hsqldb:mem:pocketETL");
    }

    @Before
    public void initializeDatabase() throws Exception {
        connection = dataSource.getConnection();
        connection.createStatement().execute(DROP_SQL);
        connection.createStatement().execute(CREATE_SQL);
    }

    @Test
    public void canReadRecordWithValues() throws Exception {
        connection.createStatement().execute("INSERT INTO test_data VALUES (1, 'test', 123, '2017-01-02 03:04:05', TRUE)");
        sqlExtractor.open(null);

        TestDTO2 expectedDTO = new TestDTO2(1, "test", 123, new DateTime(2017, 1, 2, 3, 4, 5), true);
        TestDTO2 actualDTO = sqlExtractor.next().orElseThrow(RuntimeException::new);

        assertThat(actualDTO, equalTo(expectedDTO));
    }

    @Test
    public void canReadRecordWithNullValues() throws Exception {
        connection.createStatement().execute("INSERT INTO test_data VALUES (1, null, null, null, null)");
        sqlExtractor.open(null);

        TestDTO2 expectedDTO = new TestDTO2(1, null, null, null, null);
        TestDTO2 actualDTO = sqlExtractor.next().orElseThrow(RuntimeException::new);

        assertThat(actualDTO, equalTo(expectedDTO));
    }

    @Test
    public void sqlBatchedStatementInjectionAttemptFails() throws Exception {
        connection.createStatement().execute("INSERT INTO test_data VALUES (1, null, null, null, null)");
        SqlExtractor<TestDTO2> sqlExtractorForInjection =
                SqlExtractor.of(dataSource, "SELECT * FROM test_data WHERE aNumber = #injectHere", TestDTO2.class)
                .withSqlParameters(ImmutableMap.of("injectHere", "5; DROP TABLE test_data"));

        try {
            sqlExtractorForInjection.open(null);
            fail("Exception should have been thrown");
        } catch (RuntimeException ignored) {
            // no-op
        }

        ResultSet resultSet = connection.createStatement().executeQuery("SELECT COUNT(*) FROM test_data");
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getInt(1), equalTo(1));
    }

    @Test
    public void sqlLogicHijackStatementInjectionAttemptFails() throws Exception {
        connection.createStatement().execute("INSERT INTO test_data VALUES (1, null, null, null, null)");
        SqlExtractor<TestDTO2> sqlExtractorForInjection =
                SqlExtractor.of(dataSource, "SELECT * FROM test_data WHERE aString = #injectHere", TestDTO2.class)
                        .withSqlParameters(ImmutableMap.of("injectHere", "foo' OR '1'='1"));
        sqlExtractorForInjection.open(null);

        assertThat(sqlExtractorForInjection.next(), equalTo(Optional.empty()));
    }
}
