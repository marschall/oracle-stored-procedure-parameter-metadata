package com.github.marschall.oraclestoredprocedureparametermetadata;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.context.TestConstructor.AutowireMode.ALL;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
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

  @TrueFalse
  void callProcedure(boolean bindByName) {
    GenericStoredProcedureCaller caller = new GenericStoredProcedureCaller(this.jdbcTemplate, bindByName);
    Map<String, Object> resultMap = caller.callPrecedure("property_tax", Map.of("subtotal", 100.0f));
    assertNotNull(resultMap);
    assertEquals(1, resultMap.size());
    Double result = (Double) resultMap.get("TAX");
    assertNotNull(result);
    assertEquals(6.0d, result, 0.0000001d);

//    result = this.caller.callPrecedure("stored_procedure_proxy.negate_procedure", Map.of("b", true));
  }

  @TrueFalse
  void callFunction(boolean bindByName) {
    GenericStoredProcedureCaller caller = new GenericStoredProcedureCaller(this.jdbcTemplate, bindByName);
    Map<String, Object> resultMap = caller.callFunction("sales_tax", Map.of("subtotal", 100.0f));
    assertNotNull(resultMap);
    assertEquals(1, resultMap.size());
    Double result = (Double) resultMap.get(GenericStoredProcedureCaller.RESULT_KEY);
    assertNotNull(result);
    assertEquals(6.0d, result, 0.0000001d);

//    result = this.caller.callFunction("stored_procedure_proxy.negate_function", Map.of("b", true));
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @interface TrueFalse {

  }

}
