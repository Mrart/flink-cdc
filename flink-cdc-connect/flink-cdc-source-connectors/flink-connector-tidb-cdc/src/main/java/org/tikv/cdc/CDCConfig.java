package org.tikv.cdc;

public class CDCConfig {
  private static final int EVENT_BUFFER_SIZE = 50000;
  private static final int MAX_ROW_KEY_SIZE = 10240;

  private int eventBufferSize = EVENT_BUFFER_SIZE;
  private int maxRowKeySize = MAX_ROW_KEY_SIZE;

  public void setEventBufferSize(final int bufferSize) {
    eventBufferSize = bufferSize;
  }

  public void setMaxRowKeySize(final int rowKeySize) {
    maxRowKeySize = rowKeySize;
  }

  public int getEventBufferSize() {
    return eventBufferSize;
  }

  public int getMaxRowKeySize() {
    return maxRowKeySize;
  }
}
