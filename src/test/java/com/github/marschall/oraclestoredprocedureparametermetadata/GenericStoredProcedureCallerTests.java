package com.github.marschall.oraclestoredprocedureparametermetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@ContextConfiguration(classes = OracleConfiguration.class)
class GenericStoredProcedureCallerTests {

  @Autowired
  private DataSource dataSource;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void callProcedure(boolean bindByName) {
    GenericStoredProcedureCaller caller = new GenericStoredProcedureCaller(new JdbcTemplate(this.dataSource), bindByName);
    Map<String, Object> result = caller.callPrecedure("property_tax", Map.of("subtotal", 100.0f));
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(6.0d, (Double) result.get("TAX"), 0.0000001d);

//    result = this.caller.callPrecedure("stored_procedure_proxy.negate_procedure", Map.of("b", true));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void callFunction(boolean bindByName) {
    GenericStoredProcedureCaller caller = new GenericStoredProcedureCaller(new JdbcTemplate(this.dataSource), bindByName);
    Map<String, Object> result = caller.callFunction("sales_tax", Map.of("subtotal", 100.0f));
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(6.0d, (Double) result.get("result"), 0.0000001d);
//    result = this.caller.callFunction("stored_procedure_proxy.negate_function", Map.of("b", true));
  }

}
