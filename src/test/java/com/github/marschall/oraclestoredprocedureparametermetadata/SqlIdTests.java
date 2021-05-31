package com.github.marschall.oraclestoredprocedureparametermetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SqlIdTests {

  @Test
  void sqlId() {
    String nativeSQL = "SELECT * from dual where dummy = :1 ";
    assertEquals("71hmmykrsa7wp", OriginalSqlId.SQL_ID(nativeSQL));
    assertEquals("71hmmykrsa7wp", SqlId.SQL_ID(nativeSQL));
  }

}
