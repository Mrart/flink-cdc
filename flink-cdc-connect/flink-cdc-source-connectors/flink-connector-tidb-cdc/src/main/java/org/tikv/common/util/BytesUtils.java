package org.tikv.common.util;

public class BytesUtils {

  public static int compare(byte[] a, byte[] b) {

    int length = Math.min(a.length, b.length);
    for (int i = 0; i < length; i++) {
      if (a[i] != b[i]) {
        return Byte.compare(a[i], b[i]);
      }
    }
    return Integer.compare(a.length, a.length);
  }
}
