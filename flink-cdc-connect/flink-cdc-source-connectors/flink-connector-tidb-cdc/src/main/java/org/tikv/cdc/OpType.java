package org.tikv.cdc;

public enum OpType {
  Ddl(0),
  Put(1),
  Delete(2),
  Heatbeat(3),
  Resolved(4);

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
