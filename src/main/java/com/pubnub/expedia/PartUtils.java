package com.pubnub.expedia;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

abstract class Chunk {
    @Override
    public String toString() {
        return "Chunk{" +
                "pnMessageResult=" + pnMessageResult +
                '}';
    }

    public PNMessageResult getPnMessageResult() {
        return pnMessageResult;
    }

    private final PNMessageResult pnMessageResult;

    protected Chunk(final PNMessageResult pnMessageResult) {
        this.pnMessageResult = pnMessageResult;
    }
}

class Part extends Chunk {
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Part part = (Part) o;
        return Objects.equals(shaSum, part.shaSum) && Objects.equals(partOf, part.partOf) && Objects.equals(data, part.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shaSum, partOf, data);
    }

    @Override
    public String toString() {
        return "Part{" +
                "shaSum='" + shaSum + '\'' +
                ", partOf='" + partOf + '\'' +
                ", data='" + data + '\'' +
                '}';
    }

    private final String shaSum;
    private final String partOf;
    private final String data;

    public String getShaSum() {
        return shaSum;
    }

    public String getPartOf() {
        return partOf;
    }

    public String getData() {
        return data;
    }

    Part(final PNMessageResult pnMessageResult, final String shaSum, final String partOf, final String data) {
        super(pnMessageResult);
        this.shaSum = shaSum;
        this.partOf = partOf;
        this.data = data;
    }
}

class Final extends Chunk {
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Final aFinal = (Final) o;
        return Objects.equals(shaSum, aFinal.shaSum) && Objects.equals(parts, aFinal.parts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shaSum, parts);
    }

    private final String shaSum;
    private final List<String> parts;

    Final(final PNMessageResult pnMessageResult, final String shaSum, final List<String> parts) {
        super(pnMessageResult);
        this.shaSum = shaSum;
        this.parts = parts;
    }

    @Override
    public String toString() {
        return "Final{" +
                "shaSum='" + shaSum + '\'' +
                ", parts=" + parts +
                '}';
    }

    public String getShaSum() {
        return shaSum;
    }

    public List<String> getParts() {
        return parts;
    }
}


final class PartUtils {
    private PartUtils() {}

    static Optional<String> extractString(final PNMessageResult pnMessageResult, final String key) {
        final JsonElement jsonElement = pnMessageResult.getMessage();
        if (jsonElement != null) {
            try {
                final JsonObject jsonObject = jsonElement.getAsJsonObject();
                final String type = jsonObject.get(key).getAsString();
                return Optional.of(type);
            } catch (IllegalStateException | ClassCastException ise) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    static Optional<String> extractData(final PNMessageResult pnMessageResult) {
        return extractString(pnMessageResult, "data");
    }

    static Optional<String> extractPartOf(final PNMessageResult pnMessageResult) {
        return extractString(pnMessageResult, "partof");
    }

    static Optional<String> extractShaSum(final PNMessageResult pnMessageResult) {
        return extractString(pnMessageResult, "shasum");
    }

    static Optional<String> extractType(final PNMessageResult pnMessageResult) {
        return extractString(pnMessageResult, "type");
    }

    static boolean isPartChunk(final PNMessageResult pnMessageResult) {
        return extractType(pnMessageResult)
                .map(it -> it.equals("part"))
                .orElse(false);
    }

    static boolean isFinalChunk(final PNMessageResult pnMessageResult) {
        return extractType(pnMessageResult)
                .map(it -> it.equals("final"))
                .orElse(false);
    }

    static boolean isChunk(final PNMessageResult pnMessageResult) {
        return isFinalChunk(pnMessageResult) || isPartChunk(pnMessageResult);
    }

    static Optional<Final> extractFinalChunk(final PNMessageResult pnMessageResult) {
        return extractShaSum(pnMessageResult)
                .map(shaSum -> new Final(pnMessageResult, shaSum, extractParts(pnMessageResult)));
    }

    static List<String> extractParts(final PNMessageResult pnMessageResult) {
        final JsonElement jsonElement = pnMessageResult.getMessage();
        if (jsonElement != null) {
            try {
                final JsonObject jsonObject = jsonElement.getAsJsonObject();
                final JsonArray partsJsonArray = jsonObject.get("parts").getAsJsonArray();
                final List<String> parts = new ArrayList<>();
                partsJsonArray.forEach(jsonElementFromArray -> {
                    parts.add(jsonElementFromArray.getAsString());
                });
                return parts;
            } catch (IllegalStateException | ClassCastException ise) {
                return Collections.emptyList();
            }
        } else {
            return Collections.emptyList();
        }
    }

    static Optional<Part> extractPartChunk(final PNMessageResult pnMessageResult) {
        return extractShaSum(pnMessageResult).flatMap(shaSum ->
                extractPartOf(pnMessageResult).flatMap(partOf ->
                        extractData(pnMessageResult).map(data -> new Part(pnMessageResult, shaSum, partOf, data))));
    }

}
