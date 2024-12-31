package org.tikv.cdc.frontier;

import org.tikv.kvproto.Coprocessor;

public interface Frontier {
    void forward(long regionID, Coprocessor.KeyRange span, long ts);

    long frontier();

    void entries(FrontierConsumer fn);

    String toString();
}
