package zw.co.airvote.mobile;

import org.json.JSONObject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import java.nio.charset.StandardCharsets;

/** Public, read-only compatibility check also used by the desktop launcher. */
public final class ConnectionVerifier {
    private ConnectionVerifier() {}

    public static void verify(String origin) throws Exception {
        URL address = new URL(origin + "/api/desktop/status");
        HttpsURLConnection conn = (HttpsURLConnection) address.openConnection();
        conn.setConnectTimeout(15000);
        conn.setReadTimeout(15000);
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");
        try {
            if (conn.getResponseCode() != 200) {
                throw new IOException("AirVote server returned HTTP " + conn.getResponseCode());
            }
            String contentType = conn.getContentType();
            if (contentType == null || !contentType.toLowerCase(java.util.Locale.ROOT).startsWith("application/json")) {
                throw new IOException("This server did not return the AirVote compatibility response.");
            }
            try (InputStream in = conn.getInputStream();
                 ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[512];
                int count;
                while ((count = in.read(buffer)) != -1) {
                    if (out.size() + count > 4096) {
                        throw new IOException("The AirVote server response was too large.");
                    }
                    out.write(buffer, 0, count);
                }
                JSONObject status = new JSONObject(out.toString(StandardCharsets.UTF_8.name()));
                if (!"radio-zimbabwe-voting-studio".equals(status.optString("application"))
                        || status.optInt("desktop_api", -1) != 1) {
                    throw new IOException("This address is not a compatible AirVote server.");
                }
            }
        } finally {
            conn.disconnect();
        }
    }
}
