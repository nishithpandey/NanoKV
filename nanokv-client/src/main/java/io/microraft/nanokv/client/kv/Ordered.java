package io.microraft.nanokv.client.kv;

import java.util.function.Supplier;

public interface Ordered<T> extends Supplier<T> {

    long getCommitIndex();

}
