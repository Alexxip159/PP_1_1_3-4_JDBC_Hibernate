package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    private final Connection connection = Util.getMySQLConnection();

    public UserDaoJDBCImpl() throws SQLException, ClassNotFoundException {
    }

    public void createUsersTable() throws SQLException {
        final String sql = "CREATE TABLE IF NOT EXISTS users (" +
                "id SERIAL PRIMARY KEY NOT NULL," +
                "name VARCHAR(32) NOT NULL," +
                "last_name VARCHAR(32) NOT NULL," +
                "age INT NOT NULL);";
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
    }

    public void dropUsersTable() throws SQLException {
        final String sql = "DROP TABLE IF EXISTS users;";
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        final String sql = "INSERT INTO users (" +
                "name, last_name, age) " +
                "VALUES (?, ?, ?);";
        PreparedStatement pStatement = connection.prepareStatement(sql);
        pStatement.setString(1, name);
        pStatement.setString(2, lastName);
        pStatement.setByte(3, age);
        pStatement.executeUpdate();
    }

    public void removeUserById(long id) throws SQLException {
        final String sql = "DELETE FROM users WHERE id = ?;";
        PreparedStatement pStatement = connection.prepareStatement(sql);
        pStatement.setLong(1, id);
        pStatement.executeUpdate();
    }

    public List<User> getAllUsers() throws SQLException {
        final String sql = "SELECT * FROM users;";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        List<User> users = new ArrayList<>();
        while (resultSet.next()) {
            User user = new User(
                    resultSet.getString("name"),
                    resultSet.getString("last_name"),
                    resultSet.getByte("age")
            );
            user.setId(resultSet.getLong("id"));
            users.add(user);
        }

        return users;
    }

    public void cleanUsersTable() throws SQLException {
        final String sql = "DELETE FROM users;";
        PreparedStatement pStatement = connection.prepareStatement(sql);
        pStatement.executeUpdate();
    }
}
