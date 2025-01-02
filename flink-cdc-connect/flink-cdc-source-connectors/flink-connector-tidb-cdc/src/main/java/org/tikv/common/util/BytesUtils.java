package org.tikv.common.util;

public class BytesUtils {

    public static int compare(byte[] a, byte[] b) {
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        int length = Math.min(a.length, b.length);
        for (int i = 0; i < length; i++) {
            if (a[i] != b[i]) {
                return Byte.compare(a[i], b[i]);
            }
        }
        return Integer.compare(a.length, a.length);
    }

    public static boolean equal(byte[] a, byte[] b) {
        if (a == b) return true;
        if (a == null || b == null) return false;

        int length = a.length;
        if (b.length != length) return false;

        for (int i = 0; i < length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }
}
