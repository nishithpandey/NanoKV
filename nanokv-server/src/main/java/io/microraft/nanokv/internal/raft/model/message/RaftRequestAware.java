package io.microraft.nanokv.internal.raft.model.message;

import io.microraft.nanokv.raft.proto.RaftRequest;

public interface RaftRequestAware {

    void populate(RaftRequest.Builder builder);

}
