package com.pubnub.expedia;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.pubnub.api.PubNub;
import com.pubnub.api.PubNubException;
import com.pubnub.api.builder.PubNubErrorBuilder;
import com.pubnub.api.managers.MapperManager;
import com.pubnub.api.models.consumer.PNPublishResult;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.pubnub.expedia.PublishUtils.encode;
import static com.pubnub.expedia.PublishUtils.finalChunk;
import static com.pubnub.expedia.PublishUtils.partChunk;
import static com.pubnub.expedia.PublishUtils.partition;
import static com.pubnub.expedia.PublishUtils.sha256;

public class GreedyPublish {
    private static final int BUFFER_SIZE = 32 * 1024;
    private static final int CHUNK_SIZE = 24000;

    public static class GreedyPublishResult {
        private final List<PublishPartResult> partsPNPublishResults;
        private final PNPublishResult finalPNPublishResult;

        public GreedyPublishResult(final List<PublishPartResult> partsPNPublishResults, final PNPublishResult finalPNPublishResult) {
            this.partsPNPublishResults = partsPNPublishResults;
            this.finalPNPublishResult = finalPNPublishResult;
        }

        @Override
        public String toString() {
            return "GreedyPublishResult{" +
                    "partsPNPublishResults=" + partsPNPublishResults +
                    ", finalPNPublishResult=" + finalPNPublishResult +
                    '}';
        }
    }

    private final PubNub pubNub;
    private final MapperManager mapper;

    private JsonObject message;

    private String channel;

    public GreedyPublish usePOST(final boolean usePOST) {
        return this;
    }

    public GreedyPublish channel(final String channel) {
        this.channel = channel;
        return this;
    }

    public GreedyPublish message(final String jsonObjectAsString) {
        final JsonObject dataJsonObject = JsonParser.parseString(jsonObjectAsString).getAsJsonObject();
        message(dataJsonObject);
        return this;
    }

    public GreedyPublish message(final InputStream inputStream) {
        try (final ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            int bytesRead;
            byte[] data = new byte[BUFFER_SIZE];
            while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, bytesRead);
            }
            buffer.flush();
            return message(buffer.toString());
        }
        catch (Exception e) {
            throw new IllegalStateException("something went wrong while reading json from stream", e);
        }
    }

    public GreedyPublish message(final JsonObject dataJsonObject) {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.add("timestamp", new JsonPrimitive(System.currentTimeMillis()));
        jsonObject.add("data", dataJsonObject);
        message = jsonObject;
        return this;
    }

    public GreedyPublish(final PubNub pubNub, final MapperManager mapper) {
        this.pubNub = pubNub;
        this.mapper = mapper;
    }

    public GreedyPublishResult sync() throws PubNubException {
        validate();
        final byte[] messageBytes = mapper.toJson(message).getBytes();

        final String shaSumOfWholeMessage = encode(sha256(messageBytes));

        final List<byte[]> partsAsByteArrays = partition(messageBytes, CHUNK_SIZE);

        final List<String> orderedListOfShaSums = new ArrayList<>();
        final Map<String, String> shaSumsToParts = new HashMap<>();

        partsAsByteArrays.forEach(partsBytes -> {
            final String encodedShaSumOfPart = encode(sha256(partsBytes));
            final String encodedPart = encode(partsBytes);
            orderedListOfShaSums.add(encodedShaSumOfPart);
            shaSumsToParts.put(encodedShaSumOfPart, encodedPart);
        });

        final List<PublishPartResult> partsPNPublishResults = sendPartsSync(shaSumOfWholeMessage, shaSumsToParts);
        final PNPublishResult finalPNPublishResult = sendFinalSync(shaSumOfWholeMessage, orderedListOfShaSums);

        return new GreedyPublishResult(partsPNPublishResults, finalPNPublishResult);
    }

    private PNPublishResult sendFinalSync(final String shaSumOfWholeMessage, final List<String> orderedListOfShaSums) throws PubNubException {
        final JsonObject finalChunkJsonObject = finalChunk(shaSumOfWholeMessage, orderedListOfShaSums);

        return pubNub.publish()
                .channel(channel)
                .message(finalChunkJsonObject)
                .sync();
    }

    static abstract class PublishPartResult {
        private final JsonObject jsonObject;
        protected PublishPartResult(final JsonObject jsonObject) {
            this.jsonObject = jsonObject;
        }

        static class Successful extends PublishPartResult {
            private final PNPublishResult pnPublishResult;

            protected Successful(final PNPublishResult pnPublishResult, final JsonObject jsonObject) {
                super(jsonObject);
                this.pnPublishResult = pnPublishResult;
            }
        }
        static class Failed extends PublishPartResult {
            private final PubNubException pubNubException;

            protected Failed(final PubNubException e, final JsonObject jsonObject) {
                super(jsonObject);
                this.pubNubException = e;
            }
        }
    }

    private List<PublishPartResult> sendPartsSync(final String shaSumOfWholeMessage, final Map<String, String> shaSumsToParts) {
        return shaSumsToParts.keySet().stream()
                .map(shaSum -> partChunk(shaSumOfWholeMessage, shaSum, shaSumsToParts.get(shaSum)))
                .map(partJsonObject -> {
                    try {
                        final PNPublishResult pnPublishResult = pubNub.publish()
                                .channel(channel)
                                .message(partJsonObject)
                                .sync();
                        return new PublishPartResult.Successful(pnPublishResult, partJsonObject);
                    } catch (PubNubException e) {
                        return new PublishPartResult.Failed(e, partJsonObject);
                    }
                }).collect(Collectors.toList());
    }

    private void validate() throws PubNubException {
        if (message == null) {
            throw PubNubException.builder().pubnubError(PubNubErrorBuilder.PNERROBJ_MESSAGE_MISSING).build();
        }
        if (channel == null || channel.isEmpty()) {
            throw PubNubException.builder().pubnubError(PubNubErrorBuilder.PNERROBJ_CHANNEL_MISSING).build();
        }
        if (pubNub.getConfiguration().getSubscribeKey() == null || pubNub.getConfiguration().getSubscribeKey().isEmpty()) {
            throw PubNubException.builder().pubnubError(PubNubErrorBuilder.PNERROBJ_SUBSCRIBE_KEY_MISSING).build();
        }
        if (pubNub.getConfiguration().getPublishKey() == null || pubNub.getConfiguration().getPublishKey().isEmpty()) {
            throw PubNubException.builder().pubnubError(PubNubErrorBuilder.PNERROBJ_PUBLISH_KEY_MISSING).build();
        }
    }
}
