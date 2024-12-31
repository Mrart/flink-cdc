package org.tikv.cdc.frontier;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.util.BytesUtils;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class SkipListTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SkipListTest.class);

  @Test
  public void testInsertAndRemove() {
    SkipList list = new SkipList();
    SecureRandom random = new SecureRandom();

    List<byte[]> keys = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      // Generate a random length between 1 and 128
      int length = random.nextInt(128) + 1;

      // Create a byte array with the generated length
      byte[] key = new byte[length];

      // Fill the byte array with random data
      random.nextBytes(key);

      // Add the byte array to the list
      keys.add(key);
      list.insert(key, null);
    }
    for (byte[] key : keys) {
      byte[] a = list.seek(key)[0].getKey();
      int cmp = BytesUtils.compare(a, key);
      Assert.assertEquals(0, cmp);
    }
    checkList(list);
  }

  private void checkList(SkipList list) {
    byte[] lastKey = new byte[] {};
    int nodeNum = 0;
    for (SkipListNode node = list.first(); node != null; node = node.nextAtLevel(0)) {
      if (lastKey.length != 0) {
        int cmp = BytesUtils.compare(lastKey, node.getKey());
        if (cmp > 0) {
          LOGGER.error("error");
        } else {
          Assert.assertTrue(cmp <= 0);
        }
      }
      lastKey = node.getKey();
      nodeNum++;
    }
    SkipListNode[] prevs = new SkipListNode[list.getHeight()];
    for (int i = 0; i < list.getHeight(); i++) {
      prevs[i] = list.getHead().nextAtLevel(i);
    }

    for (SkipListNode node = list.first(); node != null; node = node.next()[0]) {
      for (int i = 0; i < node.next().length; i++) {
        Assert.assertEquals(node, prevs[i]);
        prevs[i] = node.nextAtLevel(i);
      }
    }
  }

  @Test
  public void seekTest() {
    byte[] key1 = "15".getBytes();
    byte[] keyA = "a5".getBytes();
    byte[] keyB = "b5".getBytes();
    byte[] keyC = "c5".getBytes();
    byte[] keyD = "d5".getBytes();
    byte[] keyE = "e5".getBytes();
    byte[] keyF = "f5".getBytes();
    byte[] keyG = "g5".getBytes();
    byte[] keyH = "h5".getBytes();
    byte[] keyZ = "z5".getBytes();

    SkipList list = new SkipList();
    Assert.assertNull(list.seek(keyA)[0]);
    list.insert(keyC, null);
    list.insert(keyF, null);
    list.insert(keyE, null);
    list.insert(keyH, null);
    list.insert(keyG, null);
    list.insert(keyD, null);
    list.insert(keyA, null);
    list.insert(keyB, null);
    Assert.assertNull(list.seek(key1)[0].getKey());
    Assert.assertEquals(keyH, list.seek(keyH)[0].getKey());
    Assert.assertEquals(keyG, list.seek(keyG)[0].getKey());
    Assert.assertEquals(keyH, list.seek(keyZ)[0].getKey());

    Assert.assertEquals(keyA, list.seek("b0".getBytes())[0].getKey());
    Assert.assertEquals(keyB, list.seek("c0".getBytes())[0].getKey());
    Assert.assertEquals(keyC, list.seek("d0".getBytes())[0].getKey());
    Assert.assertEquals(keyD, list.seek("e0".getBytes())[0].getKey());
    Assert.assertEquals(keyE, list.seek("f0".getBytes())[0].getKey());
    Assert.assertEquals(keyF, list.seek("g0".getBytes())[0].getKey());
    Assert.assertEquals(keyG, list.seek("h0".getBytes())[0].getKey());
    Assert.assertEquals(keyH, list.seek("i0".getBytes())[0].getKey());

    Assert.assertEquals("[a5] [b5] [c5] [d5] [e5] [f5] [g5] [h5] ", list.toString());
    checkList(list);
    SkipListNode[] seekRes = list.seek("c0".getBytes());
    list.remove(seekRes, seekRes[0].nextAtLevel(0));
    Assert.assertEquals(keyB, list.seek("c0".getBytes())[0].getKey());
    //    byte[] d = list.seek("d0".getBytes())[0].getKey();
    Assert.assertEquals(keyB, list.seek("d0".getBytes())[0].getKey());
    Assert.assertEquals(keyD, list.seek("e0".getBytes())[0].getKey());
    Assert.assertEquals("[a5] [b5] [d5] [e5] [f5] [g5] [h5] ", list.toString());

    list.remove(seekRes, seekRes[0].nextAtLevel(0));

    Assert.assertEquals("[a5] [b5] [e5] [f5] [g5] [h5] ", list.toString());

    checkList(list);
    SkipListNode[] seekRes2 = list.seek("10".getBytes());
    list.remove(seekRes2, seekRes2[0].nextAtLevel(0));
    Assert.assertEquals("[b5] [e5] [f5] [g5] [h5] ", list.toString());
    checkList(list);
  }
}
