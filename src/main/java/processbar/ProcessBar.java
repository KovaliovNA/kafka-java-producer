package processbar;

import java.util.Collections;

public class ProcessBar {

  private static final int BLOCK_SIZE_IN_PERCENT = 5;
  private static final String EMPTY_BLOCK = " ";
  private static final String BLOCK = "=";
  private static final String BORDER_LEFT = "[";
  private static final String BORDER_RIGHT = "]";

  private int total;
  private int remain = 0;
  private String processBarMessage;


  public ProcessBar(int total, String processBarMessage) {
    this.total = total;
    this.processBarMessage = processBarMessage;
  }

  public synchronized void step() {
    remain++;

    if (remain > total) {
      throw new IllegalArgumentException();
    }

    int percentsDone = (100 * remain) / total;
    int printingBlocksCount = percentsDone / BLOCK_SIZE_IN_PERCENT;
    int emptyBlocksCount = (100 / BLOCK_SIZE_IN_PERCENT) - printingBlocksCount;

    String bare = BORDER_LEFT
        + repeatString(printingBlocksCount, BLOCK)
        + repeatString(emptyBlocksCount, EMPTY_BLOCK)
        + BORDER_RIGHT;

    System.out
        .print("\r" + processBarMessage + EMPTY_BLOCK + bare + EMPTY_BLOCK + percentsDone + "%");

    if (remain == total) {
      System.out.print("\n");
    }
  }

  private String repeatString(int repeatStringCount, String str) {
    return String.join("", Collections.nCopies(repeatStringCount, str));
  }
}
