package io.debezium.connector.tidb.Listeners;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.text.ParsingException;

import java.util.Collection;

public class TiDBAntlrDdlParserListener extends MySqlParserBaseListener
        implements AntlrDdlParserListener {
    @Override
    public Collection<ParsingException> getErrors() {
        return null;
    }
}
