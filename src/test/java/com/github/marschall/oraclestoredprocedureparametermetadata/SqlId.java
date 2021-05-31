package com.github.marschall.oraclestoredprocedureparametermetadata;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class SqlId {

  /**
   * Max sql_id length is 13 chars.
   */
  private static final int RESULT_SIZE = 13;

  private static final char[] ALPHABET = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

  private SqlId() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Compute sqlid for a statement, the same way as Oracle does
   * http://www.slaviks-blog.com/2010/03/30/oracle-sql_id-and-hash-value/
   * https://blog.tanelpoder.com/2009/02/22/sql_id-is-just-a-fancy-representation-of-hash-value/
   *
   * @param stmt SQL string without trailing 0x00 Byte
   * @return sql_id as computed by Oracle
   */
  static String SQL_ID(String stmt) {
    // compute MD5 sum from SQL string - including trailing 0x00 Byte
    byte[] message = stmt.getBytes(StandardCharsets.UTF_8);
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 not supported", e);
    }
    md.update(message);
    // append a trailing 0x00 byte
    md.update((byte) 0x00);
    byte[] b = md.digest();

    // bytes 0 - 7 from the hash are not used, only the last 64bits are used
    // therefore we can use a 64bit long

    // most significant unsigned int
    long sqln = ((b[11] & 0xFFl) << 56)
            | ((b[10] & 0xFFl) << 48)
            | ((b[9] & 0xFFl) << 40)
            | ((b[8] & 0xFFl) << 32)
            | ((b[15] & 0xFFl) << 24)
            | ((b[14] & 0xFFl) << 16)
            | ((b[13] & 0xFFl) << 8)
            | (b[12] & 0xFFl);

    // Compute Base32, take 13x 5bits
    // max sql_id length is 13 chars, 13 x 5 => 65bits most significant is always 0
    byte[] result = new byte[RESULT_SIZE];
    for (int i = 0; i < result.length; i++) {
      int idx = (int) (sqln & 0b11111L);
      result[result.length - i - 1] = toBase65(idx);
      sqln = sqln >>> 5;
    }
    return new String(result, StandardCharsets.ISO_8859_1); // US_ASCII fast path is only in JDK 17+
  }

  private static byte toBase65(int i) {
    return (byte) ALPHABET[i];
  }

  public static void main(String[] args) {
    System.out.println(0x100);
    System.out.println(Integer.toHexString(31));
    System.out.println(Integer.toBinaryString(31));
    System.out.println(1 << 8);
  }

}
