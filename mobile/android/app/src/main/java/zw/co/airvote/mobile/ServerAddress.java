package zw.co.airvote.mobile;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/** Validate the admin-provided server, not an arbitrary WebView URL. */
public final class ServerAddress {
    private ServerAddress() {}

    public static String normalize(String input) {
        if (input == null || input.isBlank()) {
            throw new IllegalArgumentException("Enter your station's HTTPS server address.");
        }
        String raw = input.trim();
        if (raw.chars().anyMatch(ch -> Character.isISOControl((char) ch))) {
            throw new IllegalArgumentException("The server address contains invalid characters.");
        }
        final URI uri;
        try {
            uri = new URI(raw);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Enter a valid HTTPS server address.");
        }
        if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null ||
            uri.getRawUserInfo() != null || uri.getRawQuery() != null ||
            uri.getRawFragment() != null ||
            !(uri.getRawPath() == null || uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath())) ||
            uri.getPort() > 65535 || uri.getPort() == 0) {
            throw new IllegalArgumentException("Use the server HTTPS address only, without a path or login.");
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
            host = "[" + host + "]";
        }
        return "https://" + host + (uri.getPort() == -1 ? "" : ":" + uri.getPort());
    }

    public static boolean isSameOrigin(String trustedOrigin, String candidate) {
        try {
            URI uri = new URI(candidate);
            if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null ||
                    uri.getRawUserInfo() != null) {
                return false;
            }
            URI allowed = new URI(trustedOrigin);
            int trustedPort = allowed.getPort() < 0 ? 443 : allowed.getPort();
            int candidatePort = uri.getPort() < 0 ? 443 : uri.getPort();
            return allowed.getHost().equalsIgnoreCase(uri.getHost()) && trustedPort == candidatePort;
        } catch (URISyntaxException | NullPointerException e) {
            return false;
        }
    }
}
