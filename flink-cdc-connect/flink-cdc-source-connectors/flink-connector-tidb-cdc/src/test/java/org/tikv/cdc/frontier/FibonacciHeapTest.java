package org.tikv.cdc.frontier;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Random;

public class FibonacciHeapTest {

    @Test
    public void insertTest() {
        FibonacciHeap heap = new FibonacciHeap();
        int target = 15000;
        for (int i = 0; i < 5000; i++) {
            heap.insert(10001 + target + 1);
        }
        heap.insert(target);
        Assert.assertEquals(target, heap.getMinKey());
    }

    @Test
    public void updateTsTest() {
        long seed = Instant.now().toEpochMilli();
        Random rand = new Random(seed);
        FibonacciHeap heap = new FibonacciHeap();
        FibonacciHeap.FibonacciHeapNode[] nodes = new FibonacciHeap.FibonacciHeapNode[2000];
        long expectedMin = Long.MAX_VALUE;
        for (int i = 0; i < nodes.length; i++) {
            int key = 10000 + rand.nextInt(nodes.length / 2);
            nodes[i] = heap.insert(key);
            if (expectedMin > key) {
                expectedMin = key;
            }
        }
        long key;
        for (FibonacciHeap.FibonacciHeapNode node : nodes) {
            long min = heap.getMinKey();
            Assert.assertEquals(String.format("seed:%s", seed), expectedMin, min);
            if (rand.nextInt(2) == 0) {
                key = node.getKey() + 10000;
                heap.updateKey(node, key);
            } else {
                key = node.getKey() - 10000;
                heap.updateKey(node, key);
            }
            if (expectedMin > key) {
                expectedMin = key;
            }
        }
    }
}
