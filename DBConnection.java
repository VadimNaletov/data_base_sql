import java.sql.*;
public class DBConnection {

    private static Connection connection;

    private static final String dbName = "voting";
    private static final String dbUser = "root";
    private static final String dbPass = "ca$Hmaker3";

    public static StringBuilder getInsertQuery() {
        return insertQuery;
    }

    private static StringBuilder insertQuery = new StringBuilder();

    public static Connection getConnection() {
        if (connection == null) {
            try {
                connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/" + dbName +
                        "?user=" + dbUser + "&password=" + dbPass);
                connection.createStatement().execute("DROP TABLE IF EXISTS voter_count");
                connection.createStatement().execute("CREATE TABLE voter_count(" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "name TINYTEXT NOT NULL, " +
                    "birthDate DATE NOT NULL, " +
                    "`count` INT NOT NULL, " +
                    "PRIMARY KEY(id), " +
                    "UNIQUE KEY name_date(name(50), birthDate))");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
    public static void executeMultiInsert() throws SQLException{
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO voter_count(name, birthDate, `count`) VALUES").append(insertQuery)
                .append(" ON DUPLICATE KEY UPDATE `count`=`count`+1");
        DBConnection.getConnection().createStatement().execute(sql.toString());
    }

    public static void countVoter(String name, String birthDay) throws SQLException {
        birthDay = birthDay.replace('.', '-');
        String finalBirthDay = birthDay;
        insertQuery.append(insertQuery.length() == 0 ? "" : ",")
                .append("('").append(name).append("', '").append(finalBirthDay).append("', 1)");
        if(insertQuery.length() > 100000){
            executeMultiInsert();
            insertQuery = new StringBuilder();
        }
    }

    public static void printVoterCounts() throws SQLException {
        String sql = "SELECT name, birthDate, `count` FROM voter_count WHERE `count` > 1";
        ResultSet rs = DBConnection.getConnection().createStatement().executeQuery(sql);
        while (rs.next()) {
            System.out.println(new StringBuilder().append("\t")
                    .append(rs.getString("name")).append(" (")
                    .append(rs.getString("birthDate")).append(") - ")
                    .append(rs.getInt("count")));
        }
    }
}
