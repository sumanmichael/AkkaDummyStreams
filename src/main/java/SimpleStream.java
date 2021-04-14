import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;

import java.io.IOException;
import java.sql.SQLException;

public class SimpleStream {

    public String buildSelectQuery(String tableName){
        return "SELECT * FROM "+tableName;
    }

    public String splitAndRun(String selectQuery,SourceSession sourceSession) throws SQLException, IOException {
        long OFFSET = 0;
        long LIMIT = 100;

        String csvString = sourceSession.getCSVStringFromQuery(selectQuery+"  OFFSET "+OFFSET+" ROWS FETCH NEXT "+LIMIT+" ROWS ONLY");
        return "";
    }
    public void runStream() throws SQLException {
        ActorSystem actorSystem = ActorSystem.create();
        SlickSession sourceSlickSession = SlickSession.forConfig("slick-source-postgres");
        SourceSession sourceSession = new SourceSession(sourceSlickSession);

        Source<String, NotUsed> tableNamesSource = Source.from(sourceSession.getTableNames());
        Flow<String, String, NotUsed> selectQueryWithMappedColumns = Flow.of(String.class).map(this::buildSelectQuery);
        Flow<String, String, NotUsed> splitQueryToChuckQueries = Flow.of(String.class).map(selectQuery->this.splitAndRun(selectQuery,sourceSession));
    }

    public static void main(String[] args) throws SQLException {
        new SimpleStream().runStream();
    }
}
