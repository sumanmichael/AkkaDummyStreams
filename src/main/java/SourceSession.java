import akka.stream.alpakka.slick.javadsl.SlickSession;
import com.opencsv.CSVWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SourceSession {
    SlickSession slickSession;
    Connection connection;

    public SourceSession(SlickSession slickSession) {
        this.slickSession = slickSession;
    }

    public List<String> getTableNames() throws SQLException {
        return List.of("T1","T2","T3");
    }

    public String getCSVStringFromQuery(String query) throws SQLException, IOException {
        Connection conn = this.slickSession.db().createSession().conn();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        rs.setFetchSize(1000);
        int rowcount = 0;
        if (rs.last()) {
            rowcount = rs.getRow();
            rs.beforeFirst(); // not rs.first() because the rs.next() below will move on, missing the first element
        }
        if(rowcount==0){
            return  "";
        }else {
            StringWriter sw = new StringWriter();
            CSVWriter csvWriter = new CSVWriter(sw, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER, '"', CSVWriter.DEFAULT_LINE_END);
            csvWriter.writeAll(rs, true);
            conn.close();
            return sw.toString();
        }
    }
}
