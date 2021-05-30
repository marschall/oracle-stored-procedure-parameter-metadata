package com.github.marschall.oraclestoredprocedureparametermetadata;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.context.TestConstructor.AutowireMode.ALL;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.Map;

import org.junit.jupiter.api.Test;
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

  @Test
  void nativeSql() {
    String sql = "SELECT * from dual where dummy = ?";
    String nativeSQL = this.jdbcTemplate.execute((Connection connection) -> {
      return connection.nativeSQL(sql);
    });
    assertEquals("SELECT * from dual where dummy = :1 ", nativeSQL);
    assertEquals("71hmmykrsa7wp", SQL_ID(nativeSQL));
    assertEquals("X", this.jdbcTemplate.queryForObject(sql, String.class, "X"));
  }


  /**
   * Compute sqlid for a statement, the same way as Oracle does
   * http://www.slaviks-blog.com/2010/03/30/oracle-sql_id-and-hash-value/
   * https://blog.tanelpoder.com/2009/02/22/sql_id-is-just-a-fancy-representation-of-hash-value/
   *
   * @param stmt
   *          - SQL string without trailing 0x00 Byte
   * @return sql_id as computed by Oracle
   */
  private static String SQL_ID(String stmt) {
    String result = "(sql_id)";

    try {
      // compute MD5 sum from SQL string - including trailing 0x00 Byte
      byte[] message = (stmt).getBytes("utf8");
      byte[] bytesMessage = new byte[message.length + 1];
      System.arraycopy(message, 0, bytesMessage, 0, message.length);
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] b = md.digest(bytesMessage);

      // most significant unsigned int
      long val_msb = ((((((b[11] & 0xff) * 0x100) + (b[10] & 0xff)) * 0x100)
              + (b[9] & 0xff)) * 0x100) + (b[8] & 0xff);
      val_msb = Integer.toUnsignedLong((int) val_msb);

      // least significant unsigned int
      long val_lsb = ((((((b[15] & 0xff) * 0x100) + (b[14] & 0xff)) * 0x100)
              + (b[13] & 0xff)) * 0x100) + (b[12] & 0xff);
      val_lsb = Integer.toUnsignedLong((int) val_lsb);

      // Java does not have unsigned long long, use BigInteger as bite array
      BigInteger sqln = BigInteger.valueOf(val_msb);
      sqln = sqln.shiftLeft(32);
      sqln = sqln.add(BigInteger.valueOf(val_lsb));

      // Compute Base32, take 13x 5bits
      char alphabet[] = new String("0123456789abcdfghjkmnpqrstuvwxyz").toCharArray();
      result = "";
      for (int i = 0; i < 13; i++) // max sql_id length is 13 chars, 13 x 5 =>
                                   // 65bits most significant is always 0
      {
        int idx = sqln.and(BigInteger.valueOf(31)).intValue();
        result = alphabet[idx] + result;
        sqln = sqln.shiftRight(5);
      }
    } catch (Exception e) {

    }
    return result;
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
