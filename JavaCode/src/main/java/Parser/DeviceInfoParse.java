package Parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DeviceInfoParse {
    /**
     * 此类用于解析轮胎原料信息，原料信息存放在Excel中*/
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        // Generate fileSystem
        FileSystem fileSystem = FileSystem.get(URI.create(args[0]), conf);
        //Get list of file names under the path specified by args[0]
        List<String> fileNames = new ArrayList<String>();
        fileNames = MaterialInfoParse.listAll(fileSystem,args[0]);
        //Load data into corresponding  hive tables
        loadData(fileNames);
        //Close the fileSystem
        fileSystem.close();
    }



    /**
     * Create hive table and load data from HDFS into the newly created table.
     * @param fileNames List<String>[]  List of fileNames to be loaded into hive table
     * @return void
     * */
    public static void loadData(List<String> fileNames){
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://Master:10000/linglong";
        String user = "root";
        String password = "123456";
        ResultSet res = null;
        try {
            //Step1: Load Hive Driver
            Class.forName(driver);

            //Step2: Build Hive Connector through JDBC,the port is 10000 by default.
            Connection conn = DriverManager.getConnection(url, user, password);

            //Step3: Create Statement to operate SQL.
            Statement stmt = conn.createStatement();

           //Step4: Create material table
            String tableName = "device";
            stmt.execute("drop table if exists " + tableName );
            stmt.execute("create table " + tableName + " (deviceCode string PRIMARY ,deviceName string," +
                    "deviceType string,manufactureDate string, purchaseDate string,timeOfUsage int, qualityLevel string," +
                    "purchasePrice float)" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE");

            String sql = "";
            //Load data into Hive table
            for(String filePath : fileNames){
                sql = "load data local inpath '" + filePath + "' into table " + tableName;
                System.out.println("Running: " + sql);
                stmt.execute(sql);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}