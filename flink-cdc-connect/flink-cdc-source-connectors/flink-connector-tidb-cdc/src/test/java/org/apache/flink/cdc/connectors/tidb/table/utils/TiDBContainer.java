// package org.apache.flink.cdc.connectors.tidb.table.utils;
// import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
// import org.junit.ClassRule;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.testcontainers.containers.FixedHostPortGenericContainer;
// import org.testcontainers.containers.GenericContainer;
// import org.testcontainers.containers.JdbcDatabaseContainer;
// import org.testcontainers.containers.Network;
// import org.testcontainers.containers.output.Slf4jLogConsumer;
// import org.testcontainers.utility.DockerImageName;
//
// import java.time.Duration;
// import java.util.concurrent.Future;
// import java.util.regex.Pattern;
//
// public class TiDBContainer extends  JdbcDatabaseContainer<TiDBContainer>{
//
//    private static final Logger LOG = LoggerFactory.getLogger(TiDBTestBase.class);
//    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
//
//    public static final String IMAGE = "tidb";
//
//    public static final String PD_SERVICE_NAME = "pd0";
//    public static final String TIKV_SERVICE_NAME = "tikv0";
//    public static final String TIDB_SERVICE_NAME = "tidb0";
//
//    private String TIDB_DATABASE = "test";
//    public static final String TIDB_USER = "root";
//    public static final String TIDB_PASSWORD = "";
//
//    public static final int TIDB_PORT = 4000;
//    public static final int TIKV_PORT_ORIGIN = 20160;
//    public static final int PD_PORT_ORIGIN = 2379;
//    public static int pdPort = PD_PORT_ORIGIN + 10;
//
//    public TiDBContainer() {this(TiDBVersion.V6_5);};
//    public TiDBContainer(TiDBVersion version) {
//        super(DockerImageName.parse(IMAGE + ":" + version.getVersion()));
//        addExposedPort(TIDB_PORT);
//    }
//
//    @Override
//    public String getDriverClassName() {
//        return null;
//    }
//
//    @Override
//    public String getJdbcUrl() {
//        return getJdbcUrl(TIDB_DATABASE);
//    }
//
//    public String getJdbcUrl(String databaseName) {
//        String additionalUrlParams = constructUrlParameters("?", "&");
//        return "jdbc:mysql://"
//                + getHost()
//                + ":"
//                + getDatabasePort()
//                + "/"
//                + databaseName
//                + additionalUrlParams;
//    }
//
//    public int getDatabasePort() {
//        return getMappedPort(TIDB_PORT);
//    }
//
//    @Override
//    public String getUsername() {
//        return null;
//    }
//
//    @Override
//    public String getPassword() {
//        return null;
//    }
//
//    @Override
//    protected String getTestQueryString() {
//        return null;
//    }
// }
