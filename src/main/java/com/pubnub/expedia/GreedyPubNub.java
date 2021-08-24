package com.pubnub.expedia;

import com.pubnub.api.PubNub;
import com.pubnub.api.builder.PubSubBuilder;
import com.pubnub.api.callbacks.SubscribeCallback;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

public class GreedyPubNub {
    public abstract static class ChunksAwareSubscribeCallback extends SubscribeCallback {
        private SubscribeCallback subscribeCallback;

        public SubscribeCallback getSubscribeCallback() {
            return subscribeCallback;
        }

        public void setSubscribeCallback(final SubscribeCallback subscribeCallback) {
            this.subscribeCallback = subscribeCallback;
        }
    }

    final private Collection<ChunksAwareSubscribeCallback> chunksAwareSubscribeCallbacks = Collections.synchronizedCollection(new LinkedList<>());

    final private PubNub pubNub;

    public GreedyPubNub(final PubNub pubNub) {
        this.pubNub = pubNub;
    }

    public PubSubBuilder subscribe() {
        return pubNub.subscribe();
    }

    public void addListener(final ChunksAwareSubscribeCallback listener) {
        synchronized (chunksAwareSubscribeCallbacks) {
            final ChunkJoiningSubscribeCallback chunkJoiningSubscribeCallback = new ChunkJoiningSubscribeCallback(pubNub, listener);
            listener.setSubscribeCallback(chunkJoiningSubscribeCallback);
            chunksAwareSubscribeCallbacks.add(listener);
            pubNub.addListener(chunkJoiningSubscribeCallback);
        }
    }

    void removeListener(ChunksAwareSubscribeCallback listener) {
        synchronized (chunksAwareSubscribeCallbacks) {
            if (listener.getSubscribeCallback() != null) {
                pubNub.removeListener(listener.getSubscribeCallback());
            }
            chunksAwareSubscribeCallbacks.remove(listener);
        }
    }

    public GreedyPublish publishBigChunk() {
        return new GreedyPublish(this.pubNub, pubNub.getMapper());
    }
}

