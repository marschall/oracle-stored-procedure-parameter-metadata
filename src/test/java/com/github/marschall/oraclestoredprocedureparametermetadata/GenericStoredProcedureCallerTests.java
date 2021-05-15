package com.github.marschall.oraclestoredprocedureparametermetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.context.TestConstructor.AutowireMode.ALL;

import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestConstructor;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@ContextConfiguration(classes = OracleConfiguration.class)
@TestConstructor(autowireMode = ALL)
class GenericStoredProcedureCallerTests {

  private final JdbcTemplate jdbcTemplate;

  GenericStoredProcedureCallerTests(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void callProcedure(boolean bindByName) {
    GenericStoredProcedureCaller caller = new GenericStoredProcedureCaller(this.jdbcTemplate, bindByName);
    Map<String, Object> result = caller.callPrecedure("property_tax", Map.of("subtotal", 100.0f));
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(6.0d, (Double) result.get("TAX"), 0.0000001d);

//    result = this.caller.callPrecedure("stored_procedure_proxy.negate_procedure", Map.of("b", true));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void callFunction(boolean bindByName) {
    GenericStoredProcedureCaller caller = new GenericStoredProcedureCaller(this.jdbcTemplate, bindByName);
    Map<String, Object> result = caller.callFunction("sales_tax", Map.of("subtotal", 100.0f));
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(6.0d, (Double) result.get("result"), 0.0000001d);
//    result = this.caller.callFunction("stored_procedure_proxy.negate_function", Map.of("b", true));
  }

}
