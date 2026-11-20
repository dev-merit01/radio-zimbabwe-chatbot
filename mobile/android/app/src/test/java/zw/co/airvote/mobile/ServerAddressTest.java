package zw.co.airvote.mobile;

import org.junit.Test;
import static org.junit.Assert.*;

public class ServerAddressTest {
    @Test public void acceptsHttpsOrigins() {
        assertEquals("https://airvote.example.org", ServerAddress.normalize(" https://airvote.example.org/ "));
        assertEquals("https://airvote.example.org:8443", ServerAddress.normalize("https://airvote.example.org:8443"));
    }

    @Test public void rejectsUntrustedAddresses() {
        String[] unsafe = {
            "http://airvote.example.org", "javascript:alert(1)",
            "https://attacker@airvote.example.org", "https://airvote.example.org/admin/",
            "https://airvote.example.org/?token=x", "https://airvote.example.org/#x",
            "https://airvote.example.org:99999", "https://airvote.example.org\n"
        };
        for (String address : unsafe) {
            try {
                ServerAddress.normalize(address);
                fail("Unexpected accepted address: " + address);
            } catch (IllegalArgumentException expected) {
                // The caller displays a safe validation error.
            }
        }
    }

    @Test public void limitsNavigationToConfiguredOrigin() {
        String allowed = "https://airvote.example.org:8443";
        assertTrue(ServerAddress.isSameOrigin(allowed, "https://airvote.example.org:8443/accounts/login/"));
        assertFalse(ServerAddress.isSameOrigin(allowed, "http://airvote.example.org:8443/"));
        assertFalse(ServerAddress.isSameOrigin(allowed, "https://airvote.example.org/"));
        assertFalse(ServerAddress.isSameOrigin(allowed, "https://airvote.example.org.evil.test/"));
        assertFalse(ServerAddress.isSameOrigin(allowed, "https://user@airvote.example.org:8443/"));
    }
}
