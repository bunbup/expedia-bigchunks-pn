package com.pubnub.expedia;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.objects_api.channel.PNChannelMetadataResult;
import com.pubnub.api.models.consumer.objects_api.membership.PNMembershipResult;
import com.pubnub.api.models.consumer.objects_api.uuid.PNUUIDMetadataResult;
import com.pubnub.api.models.consumer.pubsub.BasePubSubResult;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;
import com.pubnub.api.models.consumer.pubsub.PNSignalResult;
import com.pubnub.api.models.consumer.pubsub.files.PNFileEventResult;
import com.pubnub.api.models.consumer.pubsub.message_actions.PNMessageActionResult;
import com.pubnub.expedia.GreedyPubNub.ChunksAwareSubscribeCallback;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.pubnub.expedia.Message.*;
import static com.pubnub.expedia.PartUtils.extractFinalChunk;
import static com.pubnub.expedia.PartUtils.extractPartChunk;
import static com.pubnub.expedia.PartUtils.isChunk;
import static com.pubnub.expedia.PartUtils.isFinalChunk;
import static com.pubnub.expedia.PartUtils.isPartChunk;

public class ChunkJoiningSubscribeCallback extends SubscribeCallback {
    private final PubNub pubNub;

    private final ChunksAwareSubscribeCallback listener;



    private final ConcurrentHashMap<String, Message> messages = new ConcurrentHashMap<>();

    public ChunkJoiningSubscribeCallback(final PubNub pubNub, final ChunksAwareSubscribeCallback listener) {
        this.pubNub = pubNub;
        this.listener = listener;
    }

    @Override
    public void message(final PubNub pubnub, final PNMessageResult pnMessageResult) {
        if (isChunk(pnMessageResult)) {
            if (isPartChunk(pnMessageResult)) {
                extractPartChunk(pnMessageResult)
                        .ifPresent(partChunk -> {
                            addToMessages(partChunk.getPartOf(), partChunk, pnMessageResult.getTimetoken());
                            tryToDeliver(pnMessageResult, partChunk.getPartOf());
                        });
            }
            else if (isFinalChunk(pnMessageResult)) {
                extractFinalChunk(pnMessageResult)
                        .ifPresent(finalChunk -> {
                            addToMessages(finalChunk, pnMessageResult.getTimetoken());
                            tryToDeliver(pnMessageResult, finalChunk.getShaSum());
                        });
            }
            else {
                listener.message(pubnub, pnMessageResult);
            }
        }
        else {
            listener.message(pubnub, pnMessageResult);
        }
    }

    private void tryToDeliver(final PNMessageResult pnMessageResult, final String wholeMessageShaSum) {
        messages.computeIfPresent(wholeMessageShaSum, (s, message) -> {
            if (message.status() == Status.COMPLETED) {
                listener.message(pubNub, message.assemble(pnMessageResult));
                message.makeDelivered(System.currentTimeMillis());
                return message;
            }
            else {
                return message;
            }
        });
        //TODO there might some global messages lock to perform clean up and it shall deserve its own thread
        cleanDeliveredMessages();
        cleanTimedOutMessages();
    }

    private void cleanTimedOutMessages() {
        final long now = System.currentTimeMillis();
        messages.entrySet().stream()
                .filter(entry -> entry.getValue().status() == Status.FINALIZED || entry.getValue().status() == Status.PENDING)
                .filter(entry -> entry.getValue().isStaleWithRespectTo(now))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList())
                .forEach(wholeMessageShaSumToDelete -> messages.remove(wholeMessageShaSumToDelete));
    }

    private void cleanDeliveredMessages() {
        messages.entrySet().stream()
                .filter(entry -> entry.getValue().status() == Status.DELIVERED)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList())
                .forEach(wholeMessageShaSumToDelete -> messages.remove(wholeMessageShaSumToDelete));
    }

    private PNMessageResult assemble(final PNMessageResult pnMessageResult, final Message message) {
        return null;
    }

    private void addToMessages(final Final finalChunk, final Long timetoken) {
        messages.compute(finalChunk.getShaSum(), (key, message) -> {
            if (message == null) {
                return new Message(timetoken, finalChunk);
            }
            else {
                return message.finalize(timetoken, finalChunk);
            }
        });
    }

    private void addToMessages(final String wholeMessageShaSum, final Part partChunk, final Long timetoken) {
        messages.compute(wholeMessageShaSum, (key, message) -> {
            if (message == null) {
                return new Message(timetoken, partChunk);
            }
            else {
                return message.addPart(timetoken, partChunk);
            }
        });
    }

    @Override
    public void status(final PubNub pubnub, final PNStatus pnStatus) {
        listener.status(pubnub, pnStatus);
    }

    @Override
    public void presence(final PubNub pubnub, final PNPresenceEventResult pnPresenceEventResult) {
        listener.presence(pubnub, pnPresenceEventResult);
    }

    @Override
    public void signal(final PubNub pubnub, final PNSignalResult pnSignalResult) {
        listener.signal(pubnub ,pnSignalResult);
    }

    @Override
    public void uuid(final PubNub pubnub, final PNUUIDMetadataResult pnUUIDMetadataResult) {
        listener.uuid(pubnub, pnUUIDMetadataResult);
    }

    @Override
    public void channel(final PubNub pubnub, final PNChannelMetadataResult pnChannelMetadataResult) {
        listener.channel(pubnub, pnChannelMetadataResult);
    }

    @Override
    public void membership(final PubNub pubnub, final PNMembershipResult pnMembershipResult) {
        listener.membership(pubnub, pnMembershipResult);
    }

    @Override
    public void messageAction(final PubNub pubnub, final PNMessageActionResult pnMessageActionResult) {
        listener.messageAction(pubnub, pnMessageActionResult);
    }

    @Override
    public void file(final PubNub pubnub, final PNFileEventResult pnFileEventResult) {
        listener.file(pubnub, pnFileEventResult);
    }
}

class Message {
    enum Status {
        PENDING, FINALIZED, COMPLETED, DELIVERED;
    }

    private final Map<String, Part> parts = new HashMap<>();
    private Final finalPart;
    private long earliestTimeToken = Long.MAX_VALUE;
    private long latestTimeToken = 0L;
    private Long deliveredTimeToken;

    Message(final Long timetoken, final Part partChunk) {
        this.addPart(timetoken, partChunk);
    }

    Message(final Long timetoken, final Final finalChunk) {
        this.finalize(timetoken, finalChunk);
    }

    void makeDelivered(final long timeMillis) {
        this.deliveredTimeToken = timeMillis;
    }

    Status status() {
        if (deliveredTimeToken != null) {
            return Status.DELIVERED;
        }
        else if (finalPart != null && hasAllParts(finalPart)) {
            return Status.COMPLETED;
        }
        else if (finalPart != null) {
            return Status.FINALIZED;
        }
        else {
            return Status.PENDING;
        }
    }

    Message finalize(final Long timeToken, final Final finalPart) {
        if (this.finalPart != null) throw new IllegalStateException("already finalized: " + this.finalPart);
        this.finalPart = finalPart;
        updateTimeTokens(timeToken);
        return this;
    }

    private void updateTimeTokens(final Long timeToken) {
        updateEarliestTimeToken(timeToken);
        updateLatestTimeToken(timeToken);
    }

    Message addPart(final long timeToken, final Part part) {
        parts.put(part.getShaSum(), part);
        updateTimeTokens(timeToken);
        return this;
    }

    private void updateEarliestTimeToken(final long timeToken) {
        if (timeToken < earliestTimeToken) {
            earliestTimeToken = timeToken;
        }
    }

    private void updateLatestTimeToken(final long timeToken) {
        if (timeToken > latestTimeToken) {
            latestTimeToken = timeToken;
        }
    }

    private boolean hasAllParts(final Final finalPart) {
        return parts.keySet().containsAll(finalPart.getParts());
    }

    boolean isStaleWithRespectTo(final long now) {
        return now - latestTimeToken > TimeUnit.SECONDS.toMillis(30);
    }

    PNMessageResult assemble(final PNMessageResult pnMessageResult) {
        if (status() != Status.COMPLETED) throw new IllegalStateException("is not completed yet: " + this);

        final byte[] wholeMessageBytes = finalPart.getParts().stream()
                .map(partShaSum -> parts.get(partShaSum))
                .map(part -> part.getData())
                .map(encodedData -> Base64.getDecoder().decode(encodedData))
                .reduce(new byte[0], (accumulatedBytes, encodedBytes) -> {
                    final byte[] joinedBytes =  new byte[accumulatedBytes.length + encodedBytes.length];
                    System.arraycopy(accumulatedBytes, 0, joinedBytes, 0, accumulatedBytes.length);
                    System.arraycopy(encodedBytes, 0, joinedBytes, accumulatedBytes.length, encodedBytes.length);
                    return joinedBytes;
                });
        final String messageAsString = new String(wholeMessageBytes);
        final JsonObject jsonObject = JsonParser.parseString(messageAsString).getAsJsonObject();

        final BasePubSubResult basePubSubResult = new BasePubSubResult(pnMessageResult.getSubscription(),
                pnMessageResult.getActualChannel(),
                pnMessageResult.getChannel(),
                pnMessageResult.getSubscription(),
                this.earliestTimeToken,
                pnMessageResult.getUserMetadata(),
                pnMessageResult.getPublisher());
        return new PNMessageResult(basePubSubResult, jsonObject.getAsJsonObject("data"));
    }
}