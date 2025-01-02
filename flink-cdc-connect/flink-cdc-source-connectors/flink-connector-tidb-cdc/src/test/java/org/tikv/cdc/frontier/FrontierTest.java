package org.tikv.cdc.frontier;

import org.junit.Assert;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class FrontierTest {

  @Test
  public void KeyRangeFrontier() {
    ByteString keyA = ByteString.copyFrom(new byte[] {'a'});
    ByteString keyB = ByteString.copyFrom(new byte[] {'b'});
    ByteString keyC = ByteString.copyFrom(new byte[] {'c'});
    ByteString keyD = ByteString.copyFrom(new byte[] {'d'});
    Coprocessor.KeyRange krAB =
        Coprocessor.KeyRange.newBuilder().setStart(keyA).setEnd(keyB).build();
    Coprocessor.KeyRange krAC =
        Coprocessor.KeyRange.newBuilder().setStart(keyA).setEnd(keyC).build();
    Coprocessor.KeyRange krAD =
        Coprocessor.KeyRange.newBuilder().setStart(keyA).setEnd(keyD).build();
    Coprocessor.KeyRange krBC =
        Coprocessor.KeyRange.newBuilder().setStart(keyB).setEnd(keyC).build();
    Coprocessor.KeyRange krBD =
        Coprocessor.KeyRange.newBuilder().setStart(keyB).setEnd(keyD).build();
    Coprocessor.KeyRange krCD =
        Coprocessor.KeyRange.newBuilder().setStart(keyC).setEnd(keyD).build();
    Frontier frontier = new KeyRangeFrontier(5, krAD);
    Assert.assertEquals(5, frontier.frontier());
    Assert.assertEquals("[a @ 5] [d @ Max] ", frontier.toString());
    checkFrontier(frontier);
    frontier.forward(
        0,
        Coprocessor.KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {'d'}))
            .setEnd(ByteString.copyFrom(new byte[] {'e'}))
            .build(),
        100);
    Assert.assertEquals(5, frontier.frontier());
    Assert.assertEquals("[a @ 5] [d @ 100] [e @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(
        0,
        Coprocessor.KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {'g'}))
            .setEnd(ByteString.copyFrom(new byte[] {'h'}))
            .build(),
        200);
    Assert.assertEquals(5, frontier.frontier());
    Assert.assertEquals("[a @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(
        0,
        Coprocessor.KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {'a'}))
            .setEnd(ByteString.copyFrom(new byte[] {'d'}))
            .build(),
        1);
    Assert.assertEquals(1, frontier.frontier());
    Assert.assertEquals("[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(
        0,
        Coprocessor.KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {'a'}))
            .setEnd(ByteString.copyFrom(new byte[] {'d'}))
            .build(),
        2);

    Assert.assertEquals(2, frontier.frontier());
    Assert.assertEquals("[a @ 2] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);
    frontier.forward(
        0,
        Coprocessor.KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {'a'}))
            .setEnd(ByteString.copyFrom(new byte[] {'d'}))
            .build(),
        1);
    Assert.assertEquals(1, frontier.frontier());
    Assert.assertEquals("[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krBC, 3);

    Assert.assertEquals(1, frontier.frontier());
    Assert.assertEquals(
        "[a @ 1] [b @ 3] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krBC, 4);
    Assert.assertEquals(1, frontier.frontier());
    Assert.assertEquals(
        "[a @ 1] [b @ 4] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krAD, 3);
    Assert.assertEquals(3, frontier.frontier());
    Assert.assertEquals("[a @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krAB, 5);
    Assert.assertEquals(3, frontier.frontier());
    Assert.assertEquals(
        "[a @ 5] [b @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krCD, 5);
    Assert.assertEquals(3, frontier.frontier());
    Assert.assertEquals(
        "[a @ 5] [b @ 3] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krBC, 5);
    Assert.assertEquals(5, frontier.frontier());
    Assert.assertEquals(
        "[a @ 5] [b @ 5] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krAD, 6);
    Assert.assertEquals(6, frontier.frontier());
    Assert.assertEquals("[a @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krAC, 7);
    Assert.assertEquals(6, frontier.frontier());
    Assert.assertEquals(
        "[a @ 7] [c @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krBD, 8);
    Assert.assertEquals(7, frontier.frontier());
    Assert.assertEquals(
        "[a @ 7] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(0, krAB, 8);
    Assert.assertEquals(8, frontier.frontier());
    Assert.assertEquals(
        "[a @ 8] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(
        0,
        Coprocessor.KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {'1'}))
            .setEnd(ByteString.copyFrom(new byte[] {'g'}))
            .build(),
        9);
    Assert.assertEquals(9, frontier.frontier());
    Assert.assertEquals("[1 @ 9] [g @ 200] [h @ Max] ", frontier.toString());
    checkFrontier(frontier);

    frontier.forward(
        0,
        Coprocessor.KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(new byte[] {'g'}))
            .setEnd(ByteString.copyFrom(new byte[] {'i'}))
            .build(),
        10);
    Assert.assertEquals(9, frontier.frontier());
    Assert.assertEquals("[1 @ 9] [g @ 10] [i @ Max] ", frontier.toString());
    checkFrontier(frontier);
  }

  private void checkFrontier(Frontier f) {
    KeyRangeFrontier sf = (KeyRangeFrontier) f;
    List<Long> tsInlist = new ArrayList<>();
    List<Long> tsInHeap = new ArrayList<>();
    sf.getSpanList()
        .entries(
            (n) -> {
              tsInlist.add(n.getValue().getKey());
              return true;
            });
    sf.getMinTsHeap()
        .entries(
            (n) -> {
              tsInHeap.add(n.getKey());
              return true;
            });
    Assert.assertEquals(tsInlist.size(), tsInHeap.size());
    Collections.sort(tsInlist);
    Collections.sort(tsInHeap);
    Assert.assertEquals(Optional.ofNullable(tsInlist.get(0)), Optional.of(f.frontier()));
  }
}
