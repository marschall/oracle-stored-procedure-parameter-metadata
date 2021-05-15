package com.github.marschall.oraclestoredprocedureparametermetadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlInOutParameter;
import org.springframework.jdbc.core.SqlOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.GenericStoredProcedure;

public class GenericStoredProcedureCaller {

  private final JdbcTemplate jdbcTemplate;
  private final boolean bindByName;

  public GenericStoredProcedureCaller(JdbcTemplate jdbcTemplate, boolean bindByName) {
    this.jdbcTemplate = jdbcTemplate;
    this.bindByName = bindByName;
  }

  public Map<String, Object> callPrecedure(String procedureName, Map<String, Object> parameters) {

    List<SqlParameter> sqlParameters = this.jdbcTemplate.execute((Connection connection) -> {
      return this.getProcedureParameters(connection.getMetaData(), procedureName);
    });

    GenericStoredProcedure storedProcedure = new GenericStoredProcedure();
    storedProcedure.setFunction(false);

    return this.callStoredPrecedure(procedureName, parameters, storedProcedure, sqlParameters);
  }

  public Map<String, Object> callFunction(String procedureName, Map<String, Object> parameters) {
    List<SqlParameter> sqlParameters = this.jdbcTemplate.execute((Connection connection) -> {
      return this.getFunctionParameters(connection.getMetaData(), procedureName);
    });

    GenericStoredProcedure storedProcedure = new GenericStoredProcedure();
    storedProcedure.setFunction(true);

    return this.callStoredPrecedure(procedureName, parameters, storedProcedure, sqlParameters);
  }

  private Map<String, Object> callStoredPrecedure(String procedureName, Map<String, Object> parameters, GenericStoredProcedure storedProcedure, List<SqlParameter> sqlParameters) {
    storedProcedure.setSql(procedureName);
    storedProcedure.setJdbcTemplate(this.jdbcTemplate);
    for (SqlParameter sqlParameter : sqlParameters) {
      storedProcedure.declareParameter(sqlParameter);
    }

    List<SqlParameter> inParameters = extractInParameters(sqlParameters);
    if (this.bindByName) {
      return storedProcedure.execute(extractProcedureArgumentsMap(parameters, inParameters));
    } else {
      return storedProcedure.execute(extractProcedureArgumentsArray(parameters, inParameters));
    }
  }

  private List<SqlParameter> getProcedureParameters(DatabaseMetaData metaData, String procedureName) throws SQLException {
    List<SqlParameter> parameters = new ArrayList<>();
    String searchStringEscape = metaData.getSearchStringEscape();
    String catalog = extractCatalog(procedureName, searchStringEscape);
    String procedureNamePattern = extractProcedureName(procedureName, searchStringEscape);
    try (var resultSet = metaData.getProcedureColumns(catalog, null, procedureNamePattern, null)) {
      while (resultSet.next()) {
        SqlParameter sqlParameter = this.getProcedureParameter(resultSet);
        parameters.add(sqlParameter);
      }
    }
    return parameters;
  }

  private List<SqlParameter> getFunctionParameters(DatabaseMetaData metaData, String procedureName) throws SQLException {
    List<SqlParameter> parameters = new ArrayList<>();
    String searchStringEscape = metaData.getSearchStringEscape();
    String catalog = extractCatalog(procedureName, searchStringEscape);
    String procedureNamePattern = extractProcedureName(procedureName, searchStringEscape);
    try (var resultSet = metaData.getFunctionColumns(catalog, null, procedureNamePattern, null)) {
      while (resultSet.next()) {
        SqlParameter sqlParameter = this.getFunctionParameter(resultSet);
        parameters.add(sqlParameter);
      }
    }
    return parameters;
  }

  private SqlParameter getProcedureParameter(ResultSet resultSet) throws SQLException {
    String parameterName = resultSet.getString(4);
    int dataType = resultSet.getInt(6);
    short parameterType = resultSet.getShort(5);
    short scale = resultSet.getShort(10);
    switch (parameterType) {
      case DatabaseMetaData.procedureColumnIn:
        return new SqlParameter(parameterName, dataType);
      case DatabaseMetaData.procedureColumnInOut:
        return new SqlInOutParameter(parameterName, dataType);
      case DatabaseMetaData.procedureColumnOut:
        return new SqlOutParameter(parameterName, dataType);
      default:
        throw new IllegalStateException("unknown parameter type: " + parameterType);
    }
  }

  private SqlParameter getFunctionParameter(ResultSet resultSet) throws SQLException {
    String parameterName = resultSet.getString(4);
    int dataType = resultSet.getInt(6);
    short parameterType = resultSet.getShort(5);
    short scale = resultSet.getShort(10);
    switch (parameterType) {
      case DatabaseMetaData.functionColumnIn:
        return new SqlParameter(parameterName, dataType);
      case DatabaseMetaData.functionColumnResult:
        if (parameterName == null) {
          parameterName = "result";
        }
        return new SqlOutParameter(parameterName, dataType);
      default:
        throw new IllegalStateException("unknown parameter type: " + parameterType);
    }
  }

  private static List<SqlParameter> extractInParameters(List<SqlParameter> sqlInParameters) {
    return sqlInParameters.stream()
                          .filter(SqlParameter::isInputValueProvided)
                          .collect(Collectors.toList());
  }


  private static Object extractValue(Map<String, Object> parameterMap, SqlParameter sqlParameter) {
    String parameterName = sqlParameter.getName();
    Object value = parameterMap.get(parameterName);
    if (value == null) {
      value = parameterMap.get(parameterName.toLowerCase());
    }
    // TODO type conversion
    return value;
  }

  private static Object[] extractProcedureArgumentsArray(Map<String, Object> parameterMap, List<SqlParameter> sqlInParameters) {
    int argumentCount = sqlInParameters.size();
    Object[] result = new Object[argumentCount];
    int i = 0;
    for (SqlParameter sqlParameter : sqlInParameters) {
      Object value = extractValue(parameterMap, sqlParameter);
      result[i++] = value;
    }
    return result;
  }

  private static Map<String, Object> extractProcedureArgumentsMap(Map<String, Object> parameterMap, List<SqlParameter> sqlInParameters) {
    Map<String, Object> map = new HashMap<>(sqlInParameters.size());
    for (SqlParameter sqlParameter : sqlInParameters) {
      Object value = extractValue(parameterMap, sqlParameter);
      map.put(sqlParameter.getName(), value);
    }
    return map;
  }

  private static String extractCatalog(String procedureAndCatalog, String escape) {
    int dotIndex = procedureAndCatalog.indexOf('.');
    if (dotIndex != -1) {
      String catalogName = procedureAndCatalog.substring(0, dotIndex);
      return translateToNamePattern(catalogName, escape);
    } else {
      return null;
    }
  }

  private static String extractProcedureName(String procedureAndCatalog, String escape) {
    int dotIndex = procedureAndCatalog.indexOf('.');
    if (dotIndex != -1) {
      String procedureName = procedureAndCatalog.substring(dotIndex + 1);
      return translateToNamePattern(procedureName, escape);
    } else {
      return translateToNamePattern(procedureAndCatalog, escape);
    }
  }

  private static String translateToNamePattern(String s, String escape) {
    if (s == null) {
      return null;
    }
    String upperCased = s.toUpperCase();
    if (upperCased.indexOf('_') != -1) {
      return upperCased.replace("_", escape + '_');
    } else {
      return upperCased;
    }
  }

}
