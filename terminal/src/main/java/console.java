import consumer.Cs2;
import consumer.Cs3;
import producer.Pr2;
import producer.Pr3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.json.JSONObject;

public class console {

    public static void main(String[] args) throws IOException {
        //Enter data using BufferReader
        while (true){
            usage();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            // Reading data using readLine
            String command = reader.readLine();
            String[] arguments = command.split(" ");
            if (arguments.length >= 3) {
                usage();
            }
            // Get the brokers
            String brokers = "localhost:9092";
            switch (arguments[0].toLowerCase()) {
                case "help":

                    break;
                case "get_global_values":
                    command1(brokers,"Get_global_values");
                    break;
                case "get_country_values":
                    command1(brokers,"Get_country_values "+arguments[1]);
                    break;
                case "get_confirmed_avg":
                    command1(brokers,"Get_confirmed_avg");
                    break;
                case "get_deaths_avg":
                    command1(brokers,"Get_deaths_avg");
                    break;
                case "get_countries_deaths_percent":
                    command1(brokers,"Get_countries_deaths_percent");
                    break;
                default:
                    usage();
            }
        }

    }
    public static void command1(String brokers,String command) throws IOException {
        Pr2.producePr2(brokers,"Topic2",command);
        String sql = Cs2.consumeCs2(brokers,"sql","Topic2");
        String req = bdd(sql);
        Pr3.producePr3(brokers,"Topic3",req);
        Cs3.consumeCs3(brokers,"req","Topic3");
    }
    public static String bdd(String sql){
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;
        JSONObject obj = new JSONObject();
        Vector<String> columnNames = new Vector<String>();
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/covid19",
                            "postgres", "covid19_tp");
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            rs = stmt.executeQuery(sql);
            if (rs != null) {
                ResultSetMetaData columns = rs.getMetaData();
                int i = 0;
                while (i < columns.getColumnCount()) {
                    i++;
                    columnNames.add(columns.getColumnName(i));
                }

                while (rs.next()) {
                    for (i = 0; i < columnNames.size(); i++) {
                    obj.put(columnNames.get(i),rs.getString(columnNames.get(i)));
                    }
                }
            }

        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }

        finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (c != null) {
                    c.close();
                }
            } catch (Exception mysqlEx) {
                System.out.println(mysqlEx.toString());
            }

        }
        System.out.println("BDD done");

        return obj.toString();
    }

    // Display usage
    public static void usage() {
        System.out.println("commandes:");
        System.out.println("Get_global_values");
        System.out.println("Get_country_values <v_pays>");
        System.out.println("Get_confirmed_avg");
        System.out.println("Get_countries_deaths_percent");
        System.out.println("help");
    }
}
