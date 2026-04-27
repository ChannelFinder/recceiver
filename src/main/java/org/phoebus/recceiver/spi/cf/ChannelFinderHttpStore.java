package org.phoebus.recceiver.spi.cf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ChannelFinderHttpStore implements ChannelFinderStore {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI serviceUri;
    private final String authorizationHeader;

    public ChannelFinderHttpStore(String serviceUrl) {
        this(HttpClient.newHttpClient(), defaultObjectMapper(), URI.create(serviceUrl), null);
    }

    public ChannelFinderHttpStore(String serviceUrl, String username, String password) {
        this(HttpClient.newHttpClient(), defaultObjectMapper(), URI.create(serviceUrl), basicAuth(username, password));
    }

    ChannelFinderHttpStore(HttpClient httpClient, ObjectMapper objectMapper, URI serviceUri, String authorizationHeader) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.serviceUri = normalizeBaseUri(Objects.requireNonNull(serviceUri, "serviceUri"));
        this.authorizationHeader = authorizationHeader;
    }

    @Override
    public Map<String, ChannelFinderChannel> findByNames(Set<String> channelNames) {
        Map<String, ChannelFinderChannel> found = new LinkedHashMap<>();
        for (String channelName : channelNames) {
            ChannelFinderChannel channel = findByName(channelName);
            if (channel != null) {
                found.put(channelName, channel);
            }
        }
        return Map.copyOf(found);
    }

    @Override
    public void updateChannels(Collection<ChannelFinderChannel> channels) {
        if (channels.isEmpty()) {
            return;
        }
        List<CFChannel> payload = new ArrayList<>();
        for (ChannelFinderChannel channel : channels) {
            payload.add(toPayload(channel));
        }
        HttpRequest request = requestBuilder(channelsUri())
                .POST(jsonBody(payload))
                .build();
        sendExpectingSuccess(request, "update channels");
    }

    @Override
    public void deleteChannels(Collection<String> channelNames) {
        for (String channelName : channelNames) {
            HttpRequest request = requestBuilder(channelUri(channelName)).DELETE().build();
            sendExpectingSuccess(request, "delete channel " + channelName);
        }
    }

    @Override
    public void ensureProperties(Set<String> propertyNames, String owner) {
        for (String propertyName : propertyNames) {
            HttpRequest request = requestBuilder(propertyUri(propertyName))
                    .PUT(jsonBody(Map.of(
                            "name", propertyName,
                            "owner", owner
                    )))
                    .build();
            sendExpectingSuccess(request, "ensure property " + propertyName);
        }
    }

    private ChannelFinderChannel findByName(String channelName) {
        HttpRequest request = requestBuilder(channelUri(channelName)).GET().build();
        HttpResponse<String> response = send(request, "find channel " + channelName);
        int status = response.statusCode();
        if (status == 404) {
            return null;
        }
        if (status < 200 || status >= 300) {
            throw new IllegalStateException("ChannelFinder request failed while finding channel '" + channelName
                    + "' with status " + status + ": " + response.body());
        }
        try {
            CFChannel payload = objectMapper.readValue(response.body(), CFChannel.class);
            return fromPayload(payload);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode ChannelFinder channel response for '" + channelName + "'", e);
        }
    }

    private void sendExpectingSuccess(HttpRequest request, String action) {
        HttpResponse<String> response = send(request, action);
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw new IllegalStateException("ChannelFinder request failed while trying to " + action
                    + " with status " + status + ": " + response.body());
        }
    }

    private HttpResponse<String> send(HttpRequest request, String action) {
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("ChannelFinder request failed while trying to " + action, e);
        } catch (IOException e) {
            throw new IllegalStateException("ChannelFinder request failed while trying to " + action, e);
        }
    }

    private HttpRequest.Builder requestBuilder(URI uri) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(DEFAULT_TIMEOUT)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
        if (authorizationHeader != null && !authorizationHeader.isBlank()) {
            builder.header("Authorization", authorizationHeader);
        }
        return builder;
    }

    private HttpRequest.BodyPublisher jsonBody(Object value) {
        try {
            return HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(value));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode ChannelFinder request payload", e);
        }
    }

    private URI channelUri(String channelName) {
        return serviceUri.resolve("resources/channels/" + encodePathSegment(channelName));
    }

    private URI channelsUri() {
        return serviceUri.resolve("resources/channels");
    }

    private URI propertyUri(String propertyName) {
        return serviceUri.resolve("resources/properties/" + encodePathSegment(propertyName));
    }

    private static URI normalizeBaseUri(URI serviceUri) {
        String value = serviceUri.toString();
        if (!value.endsWith("/")) {
            value += "/";
        }
        return URI.create(value);
    }

    private static String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static ObjectMapper defaultObjectMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    private static String basicAuth(String username, String password) {
        String token = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        return "Basic " + token;
    }

    private static CFChannel toPayload(ChannelFinderChannel channel) {
        List<CFProperty> properties = new ArrayList<>();
        for (Map.Entry<String, String> entry : channel.properties().entrySet()) {
            properties.add(new CFProperty(entry.getKey(), channel.owner(), entry.getValue()));
        }
        return new CFChannel(channel.name(), channel.owner(), properties, List.of());
    }

    private static ChannelFinderChannel fromPayload(CFChannel payload) {
        Map<String, String> properties = new LinkedHashMap<>();
        if (payload.properties != null) {
            for (CFProperty property : payload.properties) {
                properties.put(property.name, property.value);
            }
        }
        return new ChannelFinderChannel(
                payload.name,
                payload.owner == null ? CFRecSyncProcessor.DEFAULT_OWNER : payload.owner,
                properties
        );
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class CFChannel {
        public String name;
        public String owner;
        public List<CFProperty> properties;
        public List<CFTag> tags;

        CFChannel() {
        }

        CFChannel(String name, String owner, List<CFProperty> properties, List<CFTag> tags) {
            this.name = name;
            this.owner = owner;
            this.properties = properties;
            this.tags = tags;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class CFProperty {
        public String name;
        public String owner;
        public String value;

        CFProperty() {
        }

        CFProperty(String name, String owner, String value) {
            this.name = name;
            this.owner = owner;
            this.value = value;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class CFTag {
        public String name;
        public String owner;

        CFTag() {
        }

        CFTag(String name, String owner) {
            this.name = name;
            this.owner = owner;
        }
    }
}
