package db;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JDBCTest {

    public static void main(String[] args) {


        // write data to Employees table
        writeData();

        // read the data from Employees table
        readData();

        // Read the data from file


    }// Acme

    private static void readData() {

        final String DB_URL = "jdbc:mysql://sql.computerstudi.es:3306/gc200335300";
        final String QRY = "SELECT * FROM Employees";

        Connection conn = null;
        Statement stat = null;
        ResultSet rs = null;

        try {
            // create the connection
            conn = DriverManager.getConnection(DB_URL, "gc200335300", "BVXYir6T");
            stat = conn.createStatement();
            rs = stat.executeQuery(QRY);

            ResultSetMetaData rsMetaData = rs.getMetaData();

            int numOfCols = rsMetaData.getColumnCount();
            System.out.println("Results: ");

            System.out.println(rs);

            // Store data about all the employees
            ArrayList<ArrayList<String>> dataRead = new ArrayList<>();

            // Store the data temporarily for each data in each iteration through the row
            ArrayList<String> empData = new ArrayList<>();

            // while loop to iterate the result set
            while (rs.next()) {
                System.out.println("---------Employee Data-------");
                for (int i = 1; i <= numOfCols; i++) {
                    System.out.println(rsMetaData.getColumnName(i) + ": " + rs.getString(i));
                    // added this info to the empData
                    empData.add(rs.getString(i));
                }

                // add the emp data to dataRead
                dataRead.add(empData);

                // reset empData for next employee data
                empData.clear();

                System.out.println("-----------------------------\n");
            }

            // After all the data has been read
            // Write the data to file
            writeToFile(dataRead);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTheConn(conn, stat, rs);
        }
    } // readData()

    private static void writeData() {

        final String DB_URL = "jdbc:mysql://sql.computerstudi.es:3306/gc200335300";

        String add_employee_one = "INSERT INTO Employees(firstName, lastName, age, position, year, month, day, sales, comRate) VALUES (\"James\", \"Bond\", 23, \"Manager\", 2015, 05, 13, 300.02, 3.0);";
        String add_employee_two = "INSERT INTO Employees(firstName, lastName, age, position, year, month, day, sales, comRate) VALUES (\"John\", \"Snow\", 30, \"Assistant Manager\", 2016, 01, 04, 160.02, 2.0);";

        Connection conn = null;
        Statement stat = null;

        try {
            // create the connection
            conn = DriverManager.getConnection(DB_URL, "gc200335300", "BVXYir6T");
            stat = conn.createStatement();

            // add first employee
            stat.execute(add_employee_one);
            // add second employee
            stat.execute(add_employee_two);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTheConn(conn, stat, null);
        }
    }

    private static void closeTheConn(Connection conn, Statement stat, ResultSet rs) {
        try {
            if (rs != null ) {
                rs.close();
            }
            stat.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile(ArrayList<ArrayList<String>> content) {


        // insert data to a new file
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./employee_data.txt"), "utf-8"))) {

            for (ArrayList<String> empData: content) {

                for (String info: empData) {
                    writer.write(info + "\n");
                }

                // Add the separator
                writer.write("--\n");

            }
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    private static ArrayList readFromFile() {

        // List to store all the read data
        ArrayList<ArrayList<String>> employeeData = new ArrayList<>();

        // An array to store data for each employee in file
        ArrayList<String> empData = new ArrayList<>();

        // Path to the file
        Path file = Paths.get("./employee_data.txt");

        // Instantiate a new InputStream and BufferedReader to read from file
        try (InputStream in = Files.newInputStream(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line = null;
            // Loop over content in the file and read the contents until there is no content left
            while ((line = reader.readLine()) != null) {
                if (line != "--") {
                    empData.add(line);
                } else {
                    // add the read data
                    employeeData.add(empData);
                    // empty the empData to store info of the next employee
                    empData.clear();
                }

            }
        } catch (IOException x) {
            System.err.println(x);
        }

        return employeeData;
    }

} // JDBCTest
