package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;

/**
 * ClassName: PostgreE2eITCase
 * Package: org.apache.flink.cdc.pipeline.tests
 * Description:
 *
 */
public class PostgresE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresE2eITCase.class);

    private static final DockerImageName PG_IMAGE =
            DockerImageName.parse("debezium/postgres:9.6").asCompatibleSubstituteFor("postgres");

    private static final String PG_TEST_USER = "postgres";
    private static final String PG_TEST_PASSWORD = "postgres";
    protected static final String PG_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String INTER_CONTAINER_PG_ALIAS = "postgres";
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private static final String FLINK_PROPERTIES =
            String.join(
                    "\n",
                    Arrays.asList(
                            "jobmanager.rpc.address: jobmanager",
                            "taskmanager.numberOfTaskSlots: 10",
                            "parallelism.default: 1",
                            "execution.checkpointing.interval: 10000"));

    @ClassRule
    public static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withDatabaseName("postgres")
                    .withUsername(PG_TEST_USER)
                    .withPassword(PG_TEST_PASSWORD)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_PG_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_wal_senders=20",
                            "-c",
                            "max_replication_slots=20")
                    .withReuse(true);



    @Before
    public void before() throws Exception {
        super.before();  // 启动flink容器
        initializePostgresTable("postgres_inventory");
        overrideFlinkProperties(FLINK_PROPERTIES);
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void testSyncWholeDatabase() throws Exception{
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: postgres\n"
                                + "  hostname: %s\n"
                                + "  port: 5432\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.inventory.products\n"
                                + "  slot.name: flinkcdc\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: 2",
                        INTER_CONTAINER_PG_ALIAS,
                        PG_TEST_USER,
                        PG_TEST_PASSWORD,
                        POSTGRES.getDatabaseName());

        Path postgresCdcJar = TestUtils.getResource("postgres-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        Path postgreDriverJar = TestUtils.getResource("postgresql-driver.jar");

        submitPipelineJob(pipelineJob, postgresCdcJar, valuesCdcJar, postgreDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");
    }




    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    private void initializePostgresTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = PostgresE2eITCase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try {
            Class.forName(PG_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try (Connection connection = getPgJdbcConnection();
             Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                            Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                    .map(String::trim)
                                    .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                    .map(
                                            x -> {
                                                final Matcher m =
                                                        COMMENT_PATTERN.matcher(x);
                                                return m.matches() ? m.group(1) : x;
                                            })
                                    .collect(Collectors.joining("\n"))
                                    .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getPgJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

}
