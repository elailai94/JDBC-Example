import java.io.*;
import java.sql.*;
import java.util.Scanner;
import java.math.BigDecimal;

public class JDBCExample {

    public static void main(String[] args) throws Exception {

        // loading the PostgreSQL driver
        Class.forName("org.postgresql.Driver");

        // connection to the database
        String host = args[0];
        String port = args[1];
        String databaseName = args[2];
        String username = args[3];
        String password = args[4];

        Connection connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + databaseName, username, password);

        // setting auto commit to false
        connection.setAutoCommit(false);

        // create the statement object
        Statement stmt = connection.createStatement();
        // create the tables in the database
        String createEmpTableString = "CREATE TABLE emp                      " +
                                      "  (                                   " +
                                      "     eid    NUMERIC(9, 0) PRIMARY KEY," +
                                      "     ename  VARCHAR(30),              " +
                                      "     age    NUMERIC(3, 0),            " +
                                      "     salary NUMERIC(10, 2)            " +
                                      "  )                                   "; 
        stmt.executeUpdate(createEmpTableString);

        String createDeptTableString = "CREATE TABLE dept                               " +
                                       "  (                                             " +
                                       "     did       NUMERIC(2, 0) PRIMARY KEY,       " +
                                       "     dname     VARCHAR(20),                     " +
                                       "     budget    NUMERIC(10, 2),                  " +
                                       "     managerid NUMERIC(9, 0) REFERENCES emp(eid)" +
                                       "  )                                             ";
        stmt.executeUpdate(createDeptTableString);

        String createWorksTableString = "CREATE TABLE works                          " +
                                        "  (                                         " +
                                        "     eid      NUMERIC(9, 0) REFERENCES emp, " +
                                        "     did      NUMERIC(2, 0) REFERENCES dept," +
                                        "     pct_time NUMERIC(3, 0),                " +
                                        "     PRIMARY KEY(eid, did)                  " +
                                        "  )                                         ";
        stmt.executeUpdate(createWorksTableString);

        // commit all changes to the database
        connection.commit();

        // close the statement object
        stmt.close();

        // create the prepared statement object
        String insertIntoEmpTableString = "INSERT INTO emp(eid, ename, age, salary)" +
                                          "VALUES (?, ?, ?, ?)                     ";
        PreparedStatement pstmt = connection.prepareStatement(insertIntoEmpTableString);

        // populate the emp table in the database
        File file = new File(args[5]);
        Scanner scanner = new Scanner(file);
        while(scanner.hasNextLine()) {
            String[] line = scanner.nextLine().split(",");
            BigDecimal eid = new BigDecimal(line[0]);
            String ename = line[1];
            BigDecimal age = new BigDecimal(line[2]);
            BigDecimal salary = new BigDecimal(line[3]);

            pstmt.setBigDecimal(1, eid);
            pstmt.setString(2, ename);
            pstmt.setBigDecimal(3, age);
            pstmt.setBigDecimal(4, salary);
            pstmt.executeUpdate();
        }

        // commit all changes to the database
        connection.commit();

        // close the prepared statement object
        pstmt.close();

        // create the statement object
        stmt = connection.createStatement();

        // store the function getnames in the database
        String createFunctionString = "CREATE OR REPLACE FUNCTION getnames(minsalary real)" +
                                      "RETURNS refcursor AS                               " +
                                      "$BODY$                                             " +
                                      "DECLARE mycurs refcursor;                          " +
                                      "BEGIN                                              " +
                                      "  OPEN mycurs FOR                                  " +
                                      "  SELECT DISTINCT ename                            " +
                                      "  FROM            emp                              " +
                                      "  WHERE           salary >= minsalary              " +
                                      "  ORDER BY        ename ASC;                       " +
                                      "  RETURN mycurs;                                   " +
                                      "END                                                " +
                                      "$BODY$                                             " +
                                      "LANGUAGE plpgsql;                                  ";
        stmt.executeUpdate(createFunctionString);

        // commit all changes to the database
        connection.commit();

        // close the statement object
        stmt.close();

        // create the callable statement object
        String callFunctionString = "{? = CALL getnames(?)}";
        CallableStatement cstmt = connection.prepareCall(callFunctionString);

        // call the function getnames in the database and print the results to the screen
        int salary = Integer.parseInt(args[6]);
        cstmt.setFloat(2, salary);
        cstmt.registerOutParameter(1, Types.OTHER);
        cstmt.execute();
        ResultSet rs = (ResultSet) cstmt.getObject(1);
        while(rs.next()) {
            String ename = rs.getString(1);
            System.out.println(ename);
        }

        // close the result set object
        rs.close();

        // close the callable statement object
        cstmt.close();

        // close database connection
        connection.close();
    }
}
