package io.debezium.connector.tidb;

import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.relational.SystemVariables;

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;

public class TiDBSystemVariables extends SystemVariables {

    public enum TiDBScope implements Scope {

        GLOBAL(2),
        SESSION(1),
        LOCAL(1);

        private int priority;

        TiDBScope(int priority) {
            this.priority = priority;
        }

        @Override
        public int priority() {
            return priority;
        }
    }

    /**
     * The system variable name for the name of the character set that the server uses by default.
     * See http://dev.mysql.com/doc/refman/5.7/en/server-options.html#option_mysqld_character-set-server
     */
    public static final String CHARSET_NAME_SERVER = "character_set_server";

    /**
     * The system variable name fo the name for the character set that the current database uses.
     */
    public static final String CHARSET_NAME_DATABASE = "character_set_database";
    public static final String CHARSET_NAME_CLIENT = "character_set_client";
    public static final String CHARSET_NAME_RESULT = "character_set_results";
    public static final String CHARSET_NAME_CONNECTION = "character_set_connection";

    /**
     * The system variable name to see if the MySQL tables are stored and looked-up in case sensitive way.
     * See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_lower_case_table_names
     */
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";

    public TiDBSystemVariables() {
        super(Arrays.asList(TiDBScope.SESSION, TiDBScope.GLOBAL));
    }

    @Override
    protected ConcurrentMap<String, String> forScope(Scope scope) {
        // local and session scope are the same in MySQL
        if (scope == TiDBScope.LOCAL) {
            scope = TiDBScope.SESSION;
        }
        return super.forScope(scope);
    }
}
