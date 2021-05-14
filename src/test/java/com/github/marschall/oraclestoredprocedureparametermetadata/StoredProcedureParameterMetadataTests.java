package com.github.marschall.oraclestoredprocedureparametermetadata;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.SqlInOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@ContextConfiguration(classes = OracleConfiguration.class)
class StoredProcedureParameterMetadataTests {

  @Autowired
  private DataSource dataSource;

  @Test
  @Disabled
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
      List<SqlParameter> parameters = this.getProcedureParameters(databaseMetaData, "STORED_PROCEDURE_PROXY", "NEGATE_PROCEDURE");
      assertThat(parameters, not(empty()));
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
      while (resultSet.next()) {
        procedures.add(resultSet.getString(3));
      }
    }
    return procedures;
  }

  private List<String> getProcedureColumns(DatabaseMetaData metaData) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (var resultSet = metaData.getProcedureColumns(null, null, null, null)) {
      while (resultSet.next()) {
        columns.add(resultSet.getString(3));
      }
    }
    return columns;
  }

  private List<SqlParameter> getProcedureParameters(DatabaseMetaData metaData, String packageName, String procedureName) throws SQLException {
    List<SqlParameter> parameters = new ArrayList<>();
    String searchStringEscape = metaData.getSearchStringEscape();
    try (var resultSet = metaData.getProcedureColumns(packageName.replace("_", searchStringEscape + "_"), null, procedureName.replace("_", searchStringEscape + "_"), null)) {
      while (resultSet.next()) {
        String parameterName = resultSet.getString(4);
        int dataType = resultSet.getInt(6);
        short parameterType = resultSet.getShort(5);
        short scale = resultSet.getShort(10);
        SqlParameter sqlParameter;
        switch (parameterType) {
          case DatabaseMetaData.procedureColumnIn:
            sqlParameter = new SqlParameter(parameterName, dataType);
            break;
          case DatabaseMetaData.procedureColumnInOut:
            sqlParameter = new SqlInOutParameter(parameterName, dataType);
            break;
          case DatabaseMetaData.procedureColumnOut:
            sqlParameter = new SqlInOutParameter(parameterName, dataType);
            break;
          // maybe skip procedureColumnReturn
          default:
            throw new IllegalStateException("unknown parameter type: " + parameterType);
        }
        parameters.add(sqlParameter);
      }
    }
    return parameters;
  }

  private List<String> getFunctions(DatabaseMetaData metaData) throws SQLException {
    List<String> procedures = new ArrayList<>();
    try (var resultSet = metaData.getFunctions(null, null, null)) {
      while (resultSet.next()) {
        procedures.add(resultSet.getString(3));
      }
    }
    return procedures;
  }

  private List<String> getFunctionColumns(DatabaseMetaData metaData) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (var resultSet = metaData.getFunctionColumns(null, null, null, null)) {
      while (resultSet.next()) {
        columns.add(resultSet.getString(3));
      }
    }
    return columns;
  }

}
