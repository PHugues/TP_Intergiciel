import consumer.Cs2;
import consumer.Cs3;
import producer.Pr2;
import producer.Pr3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class console {
    public static void main(String[] args) throws IOException {
        //Enter data using BufferReader
        while (true){
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            // Reading data using readLine
            String command = reader.readLine();
            String[] arguments = command.split(" ");
            if (arguments.length >= 3) {
                usage();
            }
            // Get the brokers
            String brokers = "92.148.36.86:90:9092";
            switch (arguments[0].toLowerCase()) {
                case "help":
                    usage();
                    break;
                case "get_global_values":
                    command1("92.148.36.86:9092","Get_global_values");
                    break;
                case "get_country_values":
                    command1("92.148.36.86:9092","Get_country_values "+arguments[1]);
                    break;
                case "get_confirmed_avg":
                    command1("92.148.36.86:9092","Get_confirmed_avg");
                    break;
                case "get_deaths_avg":
                    command1("92.148.36.86:9092","Get_deaths_avg");
                    break;
                case "get_countries_deaths_percent":
                    command1("92.148.36.86:9092","Get_countries_deaths_percent");
                    break;
                default:
                    usage();
            }
        }

    }
    public static void command1(String brokers,String command) throws IOException {
        Pr2.producePr2(brokers,"Topic2",command);
        String sql = Cs2.consumeCs2(brokers,"sql","Topic2");
        Map<String,String> req = bdd(sql);
        Pr3.producePr3(brokers,"Topic3",req);
        Cs3.consumeCs3(brokers,"req","Topic3");
    }
    public static Map<String,String> bdd(String sql){
        Connection c = null;
        Statement stmt = null;
        Map<String,String> map = new HashMap<>();
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://51.75.24.37:5432/covid19",
                            "demo", "covid19_tp");
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while ( rs.next() ) {
                map.put(rs.getString(1),rs.getString(2));
            }
            rs.close();
            stmt.close();
            c.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        System.out.println("BDD done");
        return map;
    }

    // Display usage
    public static void usage() {
        System.out.println("commandes:");
        System.out.println("Get_global_values");
        System.out.println("Get_country_values <v_pays>");
        System.out.println("Get_confirmed_avg");
        System.out.println("Get_countries_deaths_percent");
        System.out.println("help");
        System.exit(1);
    }
}
