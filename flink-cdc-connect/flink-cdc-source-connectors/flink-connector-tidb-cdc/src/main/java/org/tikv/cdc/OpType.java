package org.tikv.cdc;

public enum OpType {
  OpTypePut(1),
  OpTypeDelete(2),
  OpTypeResolved(3);
  private int type;

  OpType(int type) {
    this.type = type;
  }

  public int getType() {
    return this.type;
  }

  public static OpType valueOf(final int type) {
    for (OpType opType : OpType.values()) {
      if (opType.getType() == type) {
        return opType;
      }
    }
    throw new IllegalArgumentException("No enum constant with type " + type);
  }
}