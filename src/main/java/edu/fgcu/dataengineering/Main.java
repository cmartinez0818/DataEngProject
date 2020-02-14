package edu.fgcu.dataengineering;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class Main {

    private static Connection conn;

    private static String SQLQuery;


    public static void main(String[] args) throws IOException, CsvValidationException {
	      // Literally just calls our parser right now (....and is called for tests)
        CsvParser csvP = new CsvParser("src/Data/bookstore_report2.csv");
        csvP.printCsv();

        // Load the json
        /*
        1. Create instance of GSON
        2. Create a JsonReader object using FileReader
        3. Array of class instances of AuthorParser, assign data from our JsonReader
        4. foreach loop to check data
         */
        Gson gson = new Gson();
        JsonReader jread = new JsonReader(new FileReader("src/Data/authors.json"));
        AuthorParser[] authors = gson.fromJson(jread, AuthorParser[].class);

        for (var element : authors) {

            System.out.println(element.getName());
            initializeDb();
            String[] author_information = {element.getName(), element.getEmail(), element.getUrl()};
            int index = 1;
            try {
                SQLQuery = "INSERT OR IGNORE INTO author(author_name, author_email, author_url) VALUES(?,?,?);";
                PreparedStatement preparedStatement = conn.prepareStatement(SQLQuery);
                for (String s : author_information) {
                    preparedStatement.setString(index, s);
                    index++;
                }
                preparedStatement.executeUpdate();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    static void initializeDb() {

        try {
            String dbUrl = "jdbc:sqlite:src/Data/BookStore.db";

            Properties prop = new Properties();
            prop.load(new FileInputStream("src/Data/properties"));
            String pass = prop.getProperty("password");
            String user = prop.getProperty("username");
            conn = DriverManager.getConnection(dbUrl, user, pass);

        } catch (SQLException | IOException exception) {
            exception.printStackTrace();
        }
    }

    static void addBook(String isbn, String title, String author, String publisher){
        initializeDb();
        String[] book_information = {isbn, title, author, publisher};
        int index = 1;
        try {

            SQLQuery = "INSERT or ignore INTO book(isbn, book_title, author_name, publisher_name) VALUES(?,?,?,?);";
            PreparedStatement preparedStatement = conn.prepareStatement(SQLQuery);
            for (String s : book_information) {
                preparedStatement.setString(index, s);
                index++;
            }
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
