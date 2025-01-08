package org.tikv.cdc.frontier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.util.BytesUtils;

import java.util.Arrays;
import java.util.Random;

public class SkipList {
    private static final Logger LOGGER = LoggerFactory.getLogger(SkipList.class);
    public static final int MAX_HEIGHT = 12;
    public static final long MaxUint32 = (1L << 32) - 1;
    private final SkipListNode head;
    private int height;

    public SkipList() {
        this.head = new SkipListNode(null, null, MAX_HEIGHT);
        this.height = 0;
    }

    private int randomHeight() {
        int h = 1;
        Random rand = new Random();
        while (h < MAX_HEIGHT && rand.nextInt() < MaxUint32 / 4) {
            h++;
        }
        return 2;
        //    return h;
    }

    public int getHeight() {
        return height;
    }

    public SkipListNode getHead() {
        return head;
    }

    // Seek method to find the position of a given key and return the closest nodes
    public SkipListNode[] seek(byte[] key) {
        SkipListNode current = this.head;
        SkipListNode[] result = new SkipListNode[MAX_HEIGHT];
        Arrays.fill(result, null);

        for (int level = height - 1; level >= 0; level--) {
            while (true) {
                SkipListNode next = current.nextAtLevel(level);
                if (next == null) {
                    result[level] = current;
                    break;
                }
                int cmp = BytesUtils.compare(key, next.getKey());
                if (cmp < 0) {
                    result[level] = current;
                    break;
                }
                if (cmp == 0) {
                    for (; level >= 0; level--) {
                        result[level] = next;
                    }
                    return result;
                }
                current = next;
            }
        }
        return result;
    }
    // Insert the node after the seek result
    public void insert(byte[] key, FibonacciHeap.FibonacciHeapNode value) {
        SkipListNode[] seekResult = seek(key);
        insertNextToNode(seekResult, key, value);
    }

    public void insertNextToNode(
            SkipListNode[] seekResult, byte[] key, FibonacciHeap.FibonacciHeapNode value) {
        if (seekResult[0] != null
                && seekResult[0].getKey() != null
                && !nextTo(seekResult[0], key)) {
            LOGGER.error("The InsertNextToNode function can only append node to the seek result.");
            return;
        }
        int rHeight = randomHeight();
        if (this.height < rHeight) {
            this.height = rHeight;
        }
        SkipListNode n = new SkipListNode(key, value, rHeight);
        for (int level = 0; level < rHeight; level++) {

            SkipListNode prev = seekResult[level];
            if (prev == null) {
                prev = this.head;
            }
            n.setAtLevel(level, prev.nextAtLevel(level));
            prev.setAtLevel(level, n);
        }
    }

    public void remove(SkipListNode[] seekNode, SkipListNode toRemove) {
        SkipListNode current = seekNode[0];
        if (current == null || current.nextAtLevel(0) != toRemove) {
            LOGGER.error("the Remove function can only remove node right next to the seek result.");
            return;
        }
        for (int level = 0; level < toRemove.next().length; level++) {
            //      SkipListNode prevNode = this.head;
            seekNode[level].setAtLevel(level, toRemove.next()[level]);
        }
    }

    public boolean nextTo(SkipListNode node, byte[] key) {
        int cmp = BytesUtils.compare(node.getKey(), key);
        if (cmp == 0) {
            return true;
        }
        if (cmp > 0) {
            return false;
        }
        SkipListNode next = node.nextAtLevel(0);
        if (next == null) {
            return true;
        }
        return BytesUtils.compare(next.getKey(), key) > 0;
    }

    public SkipListNode first() {
        return head.nextAtLevel(0);
    }

    public void entries(EntryConsumer fn) {
        for (SkipListNode node = first(); node != null; node = node.nextAtLevel(0)) {
            if (!fn.accept(node)) {
                return;
            }
        }
    }

    // String representation of the SkipList (for testing)
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        this.entries(
                (node) -> {
                    sb.append(String.format("[%s] ", new String(node.getKey())));
                    return true;
                });
        return sb.toString();
    }
}
