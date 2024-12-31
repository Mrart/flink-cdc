package org.tikv.cdc.frontier;

import org.junit.Assert;
import org.junit.Test;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

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
        frontier.forward(
                0,
                Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFrom(new byte[] {'a'}))
                        .setEnd(ByteString.copyFrom(new byte[] {'e'}))
                        .build(),
                100);
        Assert.assertEquals(5, frontier.frontier());
        Assert.assertEquals("[d @ 100] [e @ Max] ", frontier.toString());
    }
}
