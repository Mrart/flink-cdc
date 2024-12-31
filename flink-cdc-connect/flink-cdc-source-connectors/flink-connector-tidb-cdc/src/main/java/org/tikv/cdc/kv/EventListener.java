package org.tikv.cdc.kv;

import org.tikv.cdc.exception.ClientException;
import org.tikv.cdc.model.PolymorphicEvent;

/** event listener for client process event logic. */
public interface EventListener {
    void notify(PolymorphicEvent rawKVEntry);

    void onException(ClientException e);
}
