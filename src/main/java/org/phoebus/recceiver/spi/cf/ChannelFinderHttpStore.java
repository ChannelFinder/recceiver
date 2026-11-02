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
import java.util.logging.Logger;

/**
 * HTTP client implementation of the ChannelFinderStore interface.
 *
 * <p>Communicates with a ChannelFinder REST service to query and update channels. The service URL
 * is normalized to ensure it ends with "/" so that {@link URI#resolve(String)} correctly handles
 * relative paths.</p>
 *
 * <p><strong>Endpoints:</strong>
 * <ul>
 *   <li><strong>GET /resources/channels/{name}</strong>: Fetch a single channel by name.</li>
 *   <li><strong>GET /resources/scroll</strong>: Start a scroll query by property.</li>
 *   <li><strong>GET /resources/scroll/{scrollId}</strong>: Continue a scroll query when response id is present.</li>
 *   <li><strong>POST /resources/channels</strong>: Create or update a list of channels.
 *       The request body is a JSON array of channel objects.</li>
 *   <li><strong>DELETE /resources/channels</strong>: Delete multiple channels in one request body list.</li>
 *   <li><strong>POST /resources/properties/{name}</strong>: Create a property (if it does not exist).</li>
 * </ul>
 * </p>
 */
public class ChannelFinderHttpStore implements ChannelFinderStore {
    private static final Logger log = Logger.getLogger(ChannelFinderHttpStore.class.getName());
    private static final Duration MAX_BULK_UPDATE_TIMEOUT = Duration.ofMinutes(5);
    private static final int CHANNELS_PER_TIMEOUT_STEP = 250;
    private static final Duration TIMEOUT_STEP = Duration.ofSeconds(1);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI serviceUri;
    private final String authorizationHeader;
    private final Duration timeout;

    public ChannelFinderHttpStore(String serviceUrl, Duration timeout) {
        this(HttpClient.newHttpClient(), defaultObjectMapper(), URI.create(serviceUrl), null, timeout);
    }

    public ChannelFinderHttpStore(String serviceUrl, String username, String password, Duration timeout) {
        this(HttpClient.newHttpClient(), defaultObjectMapper(), URI.create(serviceUrl), basicAuth(username, password), timeout);
    }

    ChannelFinderHttpStore(HttpClient httpClient, ObjectMapper objectMapper, URI serviceUri, String authorizationHeader,
                           Duration timeout) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.serviceUri = normalizeBaseUri(Objects.requireNonNull(serviceUri, "serviceUri"));
        this.authorizationHeader = authorizationHeader;
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
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

    /**
     * Finds channels by a property name and value.
     *
     * <p>Uses ChannelFinder scroll search with two endpoints:
     * first call `/resources/scroll`, then follow-up calls to `/resources/scroll/{scrollId}` while
     * response `id` is present. This avoids server-side result limits such as the 10k cap of
     * non-scroll queries.</p>
     *
     * @param propertyName the ChannelFinder property name to search by (e.g., "iocid").
     * @param propertyValue the property value to match.
     * @return a map of channel name -> ChannelFinderChannel. Empty if no channels match.
     * @throws IllegalStateException if the request fails or the response body cannot be decoded.
     */
    @Override
    public Map<String, ChannelFinderChannel> findByProperty(String propertyName, String propertyValue) {
        Map<String, ChannelFinderChannel> found = new LinkedHashMap<>();
        String scrollId = null;

        while (true) {
            URI uri;
            if (scrollId == null) {
                uri = startScrollChannelsUri(propertyName, propertyValue);
            } else {
                uri = continueScrollChannelsUri(scrollId, propertyName, propertyValue);
            }
            HttpRequest request = requestBuilder(uri).GET().build();
            HttpResponse<String> response = send(request, "scroll channels by property " + propertyName);
            int status = response.statusCode();
            if (status == 404) {
                return Map.copyOf(found);
            }
            if (status < 200 || status >= 300) {
                throw new IllegalStateException("ChannelFinder request failed while scrolling channels by property '"
                        + propertyName + "' with status " + status + ": " + response.body());
            }

            ScrollResult payload;
            try {
                payload = objectMapper.readValue(response.body(), ScrollResult.class);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to decode ChannelFinder scroll response for property '"
                        + propertyName + "'", e);
            }

            if (payload == null || payload.channels == null || payload.channels.isEmpty()) {
                break;
            }

            for (CFChannel channel : payload.channels) {
                if (channel != null && channel.name != null) {
                    found.put(channel.name, fromPayload(channel));
                }
            }

            String nextScrollId = normalized(payload.id);
            if (nextScrollId == null) {
                break;
            }
            if (nextScrollId.equals(scrollId)) {
                log.warning(() -> "ChannelFinder scroll returned repeated id '" + nextScrollId
                        + "' while finding property '" + propertyName + "'; stopping to avoid infinite loop");
                break;
            }
            scrollId = nextScrollId;
        }

        return Map.copyOf(found);
    }

    /**
     * Updates a collection of channels in ChannelFinder.
     *
     * <p>Constructs a POST request to `/resources/channels` with a JSON body containing all channels.
     * The ChannelFinder service automatically handles merging: if a channel exists, its properties
     * are updated; if it does not exist, it is created.</p>
     *
     * <p>Each channel is converted to a ChannelFinder-compatible payload (CFChannel) with its
     * properties serialized as a list of CFProperty objects (each with name, owner, value).</p>
     *
     * @param channels the collection of channels to create or update. If empty, this method returns
     *                 immediately without making an HTTP request.
     *
     * @throws IllegalStateException if the HTTP request fails or returns a non-2xx status.
     */
    @Override
    public void updateChannels(Collection<ChannelFinderChannel> channels) {
        if (channels.isEmpty()) {
            return;
        }
        int channelCount = channels.size();
        Duration timeout = bulkUpdateTimeout(channelCount);
        log.info(() -> "Updating ChannelFinder with " + channelCount + " channels (timeout="
                + timeout.toSeconds() + "s)");

        List<CFChannel> payload = new ArrayList<>();
        for (ChannelFinderChannel channel : channels) {
            payload.add(toPayload(channel));
        }
        HttpRequest request = requestBuilder(channelsUri(), timeout)
                .POST(jsonBody(payload))
                .build();
        sendExpectingSuccess(request, "update channels");
        log.info(() -> "Completed uploading to ChannelFinder with " + channelCount + " channels");
    }

    @Override
    public void deleteChannels(Collection<String> channelNames) {
        if (channelNames.isEmpty()) {
            return;
        }

        List<String> names = new ArrayList<>(channelNames);
        HttpRequest request = requestBuilder(channelsUri(), bulkUpdateTimeout(names.size()))
                .method("DELETE", jsonBody(names))
                .build();
        sendExpectingSuccess(request, "delete channels " + names.size());
    }

    @Override
    public void ensureProperties(Set<String> propertyNames, String owner) {
        for (String propertyName : propertyNames) {
            HttpRequest request = requestBuilder(propertyUri(propertyName))
                    .POST(jsonBody(Map.of(
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
        return requestBuilder(uri, timeout);
    }

    private HttpRequest.Builder requestBuilder(URI uri, Duration timeout) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
        if (authorizationHeader != null && !authorizationHeader.isBlank()) {
            builder.header("Authorization", authorizationHeader);
        }
        return builder;
    }

    // Scale request timeout with batch size: base + 1s per 250 channels, capped to avoid unbounded waits.
    private Duration bulkUpdateTimeout(int channelCount) {
        long extraSeconds = Math.max(0, channelCount) / CHANNELS_PER_TIMEOUT_STEP;
        Duration scaled = timeout.plus(TIMEOUT_STEP.multipliedBy(extraSeconds));
        return scaled.compareTo(MAX_BULK_UPDATE_TIMEOUT) > 0 ? MAX_BULK_UPDATE_TIMEOUT : scaled;
    }

    private HttpRequest.BodyPublisher jsonBody(Object value) {
        try {
            return HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(value));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode ChannelFinder request payload", e);
        }
    }

    /**
     * Constructs the URI for a single channel endpoint.
     *
     * <p>Format: `/resources/channels/{channelName}` (URL-encoded).</p>
     *
     * @param channelName the channel name to encode in the path.
     * @return the full URI for this channel.
     */
    private URI channelUri(String channelName) {
        return serviceUri.resolve("resources/channels/" + encodePathSegment(channelName));
    }

    /**
     * Constructs the URI for the channels collection endpoint.
     *
     * <p>Format: `/resources/channels`.</p>
     *
     * @return the full URI for bulk channel operations.
     */
    private URI channelsUri() {
        return serviceUri.resolve("resources/channels");
    }

    /**
     * Constructs the URI for starting a scroll query by property.
     *
     * <p>Format: `/resources/scroll?propertyName=propertyValue`.</p>
     */
    private URI startScrollChannelsUri(String propertyName, String propertyValue) {
        StringBuilder query = new StringBuilder("resources/scroll?")
                .append(encodePathSegment(propertyName)).append("=").append(encodePathSegment(propertyValue));
        return serviceUri.resolve(query.toString());
    }

    /**
     * Constructs the URI for continuing a scroll query with a scroll id.
     *
     * <p>Format: `/resources/scroll/{scrollId}?propertyName=propertyValue`.</p>
     */
    private URI continueScrollChannelsUri(String scrollId, String propertyName, String propertyValue) {
        StringBuilder query = new StringBuilder("resources/scroll/")
                .append(encodePathSegment(scrollId))
                .append("?")
                .append(encodePathSegment(propertyName)).append("=").append(encodePathSegment(propertyValue));
        return serviceUri.resolve(query.toString());
    }

    /**
     * Constructs the URI for a property endpoint.
     *
     * <p>Format: `/resources/properties/{propertyName}` (URL-encoded).</p>
     *
     * @param propertyName the property name to encode in the path.
     * @return the full URI for this property.
     */
    private URI propertyUri(String propertyName) {
        return serviceUri.resolve("resources/properties/" + encodePathSegment(propertyName));
    }

    /**
     * Normalizes the ChannelFinder service URI by ensuring it ends with "/".
     *
     * <p>This allows {@link URI#resolve(String)} to correctly resolve relative paths. For example:
     * <ul>
     *   <li>Input: "http://localhost:9090/ChannelFinder"</li>
     *   <li>Output: "http://localhost:9090/ChannelFinder/"</li>
     * </ul>
     * </p>
     *
     * @param serviceUri the service URI to normalize.
     * @return the normalized URI (guaranteed to end with "/").
     */
    private static URI normalizeBaseUri(URI serviceUri) {
        String value = serviceUri.toString();
        if (!value.endsWith("/")) {
            value += "/";
        }
        return URI.create(value);
    }

    /**
     * URL-encodes a path segment (channel name, property name) for safe inclusion in a URI path.
     *
     * <p>Handles special characters and ensures the segment is percent-encoded.</p>
     *
     * @param value the value to encode.
     * @return the URL-encoded segment.
     */
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

    private static String normalized(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class ScrollResult {
        public String id;
        public List<CFChannel> channels;

        ScrollResult() {
        }
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
