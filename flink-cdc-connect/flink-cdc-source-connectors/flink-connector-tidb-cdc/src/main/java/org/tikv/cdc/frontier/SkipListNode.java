package org.tikv.cdc.frontier;

// SkipListNode class represents a node in the SkipList
public class SkipListNode {
  private final byte[] key;
  private final FibonacciHeap.FibonacciHeapNode value;
  private byte[] end;
  private long region;
  private final SkipListNode[] nexts;

  public SkipListNode(byte[] key, FibonacciHeap.FibonacciHeapNode value, int height) {
    this.key = key;
    this.value = value;
    this.nexts = new SkipListNode[height];
    for (int i = 0; i < height; i++) {
      this.nexts[i] = null;
    }
  }

  public byte[] getKey() {
    return key;
  }

  public long getRegion() {
    return region;
  }

  public void setEnd(byte[] end) {
    this.end = end;
  }

  public void setRegion(long region) {
    this.region = region;
  }

  public FibonacciHeap.FibonacciHeapNode getValue() {
    return value;
  }

  public byte[] getEnd() {
    return end;
  }

  public SkipListNode[] next() {
    return nexts;
  }

  public SkipListNode nextAtLevel(int level) {
    return nexts[level];
  }

  public void setAtLevel(int level, SkipListNode node) {
    nexts[level] = node;
  }
}
