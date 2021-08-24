package test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.PubNubException;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.expedia.ChunksAwareSubscribeCallbackAdapter;
import com.pubnub.expedia.GreedyPubNub;
import com.pubnub.expedia.GreedyPublish.GreedyPublishResult;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.pubnub.expedia.GreedyPubNub.ChunksAwareSubscribeCallback;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GreedyPubNubTest {
    private static final String PUBLISH_KEY = System.getenv("PUB_KEY");
    private static final String SUBSCRIBE_KEY = System.getenv("SUB_KEY");
    private static final String CHANNEL = "channel";

    @Test
    @Disabled
    public void receive() throws InterruptedException {
        final GreedyPubNub gpn = greedyPubNub();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final List<JsonElement> messagesReceived = new ArrayList<>();

        final ChunksAwareSubscribeCallback listener = new ChunksAwareSubscribeCallbackAdapter() {
            @Override
            public void status(final PubNub pubnub, final PNStatus pnStatus) {
                System.out.println(pnStatus);
            }

            @Override
            public void message(final PubNub pubnub, final PNMessageResult pnMessageResult) {
                messagesReceived.add(pnMessageResult.getMessage());
                countDownLatch.countDown();
            }
        };

        gpn.addListener(listener);

        gpn.subscribe()
                .channels(singletonList(CHANNEL))
                .execute();

        countDownLatch.await(10, TimeUnit.SECONDS);
        System.out.println(messagesReceived);
    }

    @Test
    @Disabled
    public void publish() throws PubNubException {
        final GreedyPubNub gpn = greedyPubNub();

        final GreedyPublishResult greedyPublishResult = gpn.publishBigChunk()
                .channel(CHANNEL)
                .message(getClass().getResourceAsStream("/payload.json"))
                .sync();

        System.out.println(greedyPublishResult);
    }


    @Test
    public void endToEnd() throws InterruptedException, PubNubException {
        final GreedyPubNub gpnReceiver = greedyPubNub();
        final GreedyPubNub gpnSender = greedyPubNub();

        final CountDownLatch receivedCountDownLatch = new CountDownLatch(1);
        final CountDownLatch connectedCountDownLatch = new CountDownLatch(1);
        final List<JsonElement> messagesReceived = new ArrayList<>();

        final List<PNMessageResult> receivedMessageResults = Collections.emptyList();

        final PNMessageResult[] expectedPnMessageResult = {null};

        gpnReceiver.addListener(new ChunksAwareSubscribeCallbackAdapter() {
            @Override
            public void status(final PubNub pubnub, final PNStatus pnStatus) {
                if (pnStatus.getCategory() == PNStatusCategory.PNConnectedCategory) {
                    connectedCountDownLatch.countDown();
                }
            }

            @Override
            public void message(final PubNub pubnub, final PNMessageResult pnMessageResult) {
                messagesReceived.add(pnMessageResult.getMessage());
                expectedPnMessageResult[0] = pnMessageResult;
                receivedCountDownLatch.countDown();
            }
        });

        connectedCountDownLatch.await(10, TimeUnit.SECONDS);

        gpnReceiver.subscribe()
                .channels(singletonList(CHANNEL))
                .execute();

        gpnSender.publishBigChunk()
                .channel(CHANNEL)
                .message(getClass().getResourceAsStream("/payload.json"))
                .sync();

        receivedCountDownLatch.await(10, TimeUnit.SECONDS);
        assertNotNull(expectedPnMessageResult[0]);
        final JsonObject receivedJsonObject = expectedPnMessageResult[0].getMessage().getAsJsonObject();
        assertNotNull(receivedJsonObject);
    }

    private GreedyPubNub greedyPubNub() {
        final PNConfiguration pnConfiguration = new PNConfiguration();
        pnConfiguration.setPublishKey(PUBLISH_KEY);
        pnConfiguration.setSubscribeKey(SUBSCRIBE_KEY);

        return new GreedyPubNub(new PubNub(pnConfiguration));
    }
}
