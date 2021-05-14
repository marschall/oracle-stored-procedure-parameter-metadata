package com.github.marschall.oraclestoreprocedureparametermetadata;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@ContextConfiguration(classes = OracleConfiguration.class)
class StoredProcedureParameterMetadataTests {

  @Autowired
  private DataSource dataSource;

  @Test
  void getProcedures() throws SQLException {
    try (var connection = this.dataSource.getConnection()) {
      var databaseMetaData = connection.getMetaData();
      List<String> procedures = this.getProcedures(databaseMetaData);
      assertThat(procedures, not(empty()));
    }
  }

  @Test
  void getProcedureColumns() throws SQLException {
    try (var connection = this.dataSource.getConnection()) {
      var databaseMetaData = connection.getMetaData();
      List<String> columns = this.getProcedureColumns(databaseMetaData);
      assertThat(columns, not(empty()));
    }
  }

  @Test
  void getFunctions() throws SQLException {
    try (var connection = this.dataSource.getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      List<String> functions = this.getFunctions(metaData);
      assertThat(functions, not(empty()));
    }
  }

  @Test
  void getFunctionColumns() throws SQLException {
    try (var connection = this.dataSource.getConnection()) {
      var databaseMetaData = connection.getMetaData();
      List<String> columns = this.getFunctionColumns(databaseMetaData);
      assertThat(columns, not(empty()));
    }
  }

  @Test
  void getParameterMetaData_ForProcedure() throws SQLException {
    try (var connection = this.dataSource.getConnection();
         var callableStatement = connection.prepareCall("{call property_tax(?, ?)}")) {
      var parameterMetaData = callableStatement.getParameterMetaData();
      int parameterCount = parameterMetaData.getParameterCount();
      for (int i = 0; i < parameterCount; i++) {
        int parameterIndex = i + 1; // JDBC uses 1 based indexes
        int parameterMode = parameterMetaData.getParameterMode(parameterIndex);
        int parameterType = parameterMetaData.getParameterType(parameterIndex);
      }
    }
  }

  @Test
  void getParameterMetaData_ForFunction() throws SQLException {
    try (var connection = this.dataSource.getConnection();
            var callableStatement = connection.prepareCall("{ ? = call sales_tax(?)}")) {
      var parameterMetaData = callableStatement.getParameterMetaData();
      int parameterCount = parameterMetaData.getParameterCount();
      for (int i = 0; i < parameterCount; i++) {
        int parameterIndex = i + 1; // JDBC uses 1 based indexes
        int parameterMode = parameterMetaData.getParameterMode(parameterIndex);
        int parameterType = parameterMetaData.getParameterType(parameterIndex);
      }
    }
  }

  private List<String> getProcedures(DatabaseMetaData metaData) throws SQLException {
    List<String> procedures = new ArrayList<>();
    try (var resultSet = metaData.getProcedures(null, null, null)) {
      procedures.add(resultSet.getString(3));
    }
    return procedures;
  }

  private List<String> getProcedureColumns(DatabaseMetaData metaData) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (var resultSet = metaData.getProcedureColumns(null, null, null, null)) {
      columns.add(resultSet.getString(3));
    }
    return columns;
  }

  private List<String> getFunctions(DatabaseMetaData metaData) throws SQLException {
    List<String> procedures = new ArrayList<>();
    try (var resultSet = metaData.getFunctions(null, null, null)) {
      procedures.add(resultSet.getString(3));
    }
    return procedures;
  }

  private List<String> getFunctionColumns(DatabaseMetaData metaData) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (var resultSet = metaData.getFunctionColumns(null, null, null, null)) {
      columns.add(resultSet.getString(3));
    }
    return columns;
  }

}
