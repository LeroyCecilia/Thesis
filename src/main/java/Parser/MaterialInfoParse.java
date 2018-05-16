package Parser;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class MaterialInfoParse {
    /**
     * 此类用于解析轮胎原料信息，原料信息存放在Excel中*/
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        // Generate fileSystem
        FileSystem fileSystem = FileSystem.get(URI.create(args[0]), conf);
        //Get list of file names under the path specified by args[0]
        List<String> fileNames = new ArrayList<String>();
        fileNames = listAll(fileSystem,args[0]);
        //Load data into corresponding  hive tables
        loadData(fileNames);
        //Close the fileSystem
        fileSystem.close();
    }




    /**
     * List files/directories/links names under a directory, not include embed
     * objects
     *
     * @param fileSystem general fileSystem
     * @param dir pathName
     * @return List<String> list of file names
     * @throws IOException file io exception
     */
    public static List<String> listAll(FileSystem fileSystem,String dir) throws IOException {

        FileStatus[] stats = fileSystem.listStatus(new Path(dir));
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < stats.length; ++i) {
            if (stats[i].isFile()) {
                // regular file
                names.add(stats[i].getPath().toString());
            }
        }
        fileSystem.close();
        return names;
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
            String tableName = "material";
            stmt.execute("drop table if exists " + tableName );
            stmt.execute("create table " + tableName + " (materialCode string PRIMARY ,materialName string," +
                    "qualityLevel string,elasticity float, airTightNess float,tearStrength float, abradability float," +
                    "heatResistance float, freezeResistance float,waterResistance float,acidResistance float," +
                    "alkaliResistance float,density float,insualtionResistance float,volumeResistance float)" +
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");

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