package com.pubnub.expedia;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public final class PublishUtils {
    private PublishUtils() {}

    static List<byte[]> partition(final byte[] messageBytes, final int chunkSize) {
        final List<byte[]> listOfBytearrays = new ArrayList<>();

        for (int i = 0; i < messageBytes.length; i += chunkSize) {
            int end = i + chunkSize;

            if (end > messageBytes.length) {
                end = messageBytes.length;
            }

            listOfBytearrays.add(Arrays.copyOfRange(messageBytes, i, end));
        }

        return listOfBytearrays;
    }

    static String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    static byte[] sha256(final byte[] bytes) {
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            final byte[] digestBytes = md.digest(bytes);
            return digestBytes;
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("that shall never happen", e);
        }
    }

    static JsonObject finalChunk(final String shaSumOfWholeMessage, final List<String> orderedListOfShaSums) {
        final JsonObject finalChunkJsonObject = new JsonObject();

        finalChunkJsonObject.addProperty("type", "final");
        finalChunkJsonObject.addProperty("shasum", shaSumOfWholeMessage);
        final JsonArray jsonArrayOfPartsShaSums = new JsonArray();
        orderedListOfShaSums.forEach(partShaSumString -> jsonArrayOfPartsShaSums.add(partShaSumString));
        finalChunkJsonObject.add("parts", jsonArrayOfPartsShaSums);
        return finalChunkJsonObject;
    }

    static JsonObject partChunk(final String shaSumOfWholeMessage, final String partShaSum, final String encodedPartData) {
        final JsonObject partJsonObject = new JsonObject();
        partJsonObject.addProperty("type", "part");
        partJsonObject.addProperty("partof", shaSumOfWholeMessage);
        partJsonObject.addProperty("shasum", partShaSum);
        partJsonObject.addProperty("data", encodedPartData);
        return partJsonObject;
    }


}
