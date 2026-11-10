package zw.co.airvote.mobile;

import android.app.Activity;
import android.app.DownloadManager;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.view.View;
import android.webkit.CookieManager;
import android.webkit.DownloadListener;
import android.webkit.PermissionRequest;
import android.webkit.SslErrorHandler;
import android.net.http.SslError;
import android.webkit.URLUtil;
import android.webkit.ValueCallback;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceError;
import android.webkit.WebResourceRequest;
import android.webkit.WebResourceResponse;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.PopupMenu;
import android.widget.TextView;
import android.widget.Toast;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Native Android shell for the existing Railway-hosted AirVote staff workspace.
 * All authorisation, chart management and voting remain on the Django server.
 * Never inject JavaScript bridges or ship provider/API credentials in this app.
 */
public final class MainActivity extends Activity {
    private static final String PREFS = "airvote_connection";
    private static final String KEY_SERVER = "trusted_server";
    private static final int FILE_CHOOSER_CODE = 717;

    private enum Screen { SETUP, LOADING, WEB, ERROR }

    private final ExecutorService verifier = Executors.newSingleThreadExecutor();
    private WebView webView;
    private EditText serverField;
    private TextView loadingMessage;
    private TextView errorMessage;
    private View setupPanel;
    private View loadingPanel;
    private View errorPanel;
    private String trustedOrigin;
    private int connectionSequence = 0;
    private boolean connecting = false;
    private boolean pageLoadFailed = false;
    private Screen screen = Screen.SETUP;
    private ValueCallback<Uri[]> fileCallback;

    @Override
    public void onCreate(Bundle savedState) {
        super.onCreate(savedState);
        setContentView(R.layout.activity_main);
        webView = findViewById(R.id.station_webview);
        serverField = findViewById(R.id.server_address);
        loadingMessage = findViewById(R.id.loading_message);
        errorMessage = findViewById(R.id.error_message);
        setupPanel = findViewById(R.id.setup_panel);
        loadingPanel = findViewById(R.id.loading_panel);
        errorPanel = findViewById(R.id.error_panel);
        configureWebView();
        findViewById(R.id.connect_button).setOnClickListener(v ->
            verifyAndOpen(serverField.getText().toString()));
        findViewById(R.id.retry_button).setOnClickListener(v -> {
            String saved = getPreferences(MODE_PRIVATE).getString(KEY_SERVER, "");
            if (saved.isEmpty()) showSetup();
            else verifyAndOpen(saved);
        });
        findViewById(R.id.change_server_button).setOnClickListener(v -> showSetup());
        ImageButton options = findViewById(R.id.options_button);
        options.setOnClickListener(this::showOptions);

        String saved = getPreferences(MODE_PRIVATE).getString(KEY_SERVER, "");
        if (saved.isEmpty()) showSetup();
        else verifyAndOpen(saved);
    }

    private void configureWebView() {
        WebView.setWebContentsDebuggingEnabled(false);
        WebSettings settings = webView.getSettings();
        settings.setJavaScriptEnabled(true); // AirVote's existing staff workspace needs JavaScript.
        settings.setDomStorageEnabled(true);
        settings.setAllowFileAccess(false);
        settings.setAllowContentAccess(false);
        settings.setJavaScriptCanOpenWindowsAutomatically(false);
        settings.setSupportMultipleWindows(false);
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_NEVER_ALLOW);
        settings.setSafeBrowsingEnabled(true);
        settings.setGeolocationEnabled(false);
        settings.setBuiltInZoomControls(false);
        settings.setDisplayZoomControls(false);
        CookieManager.getInstance().setAcceptCookie(true);
        CookieManager.getInstance().setAcceptThirdPartyCookies(webView, false);
        webView.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                if (!request.isForMainFrame()) return false;
                String destination = request.getUrl().toString();
                if (trustedOrigin != null &&
                        ServerAddress.isSameOrigin(trustedOrigin, destination)) return false;
                // Cross-origin navigation is never rendered inside the signed-in workspace.
                // Explicit HTTPS links can open in the system browser, without staff cookies.
                if (request.hasGesture() && "https".equalsIgnoreCase(request.getUrl().getScheme())) {
                    try {
                        startActivity(new Intent(Intent.ACTION_VIEW, request.getUrl())
                                .addCategory(Intent.CATEGORY_BROWSABLE));
                    } catch (Exception ignored) {
                        Toast.makeText(MainActivity.this, "No browser is available.", Toast.LENGTH_SHORT).show();
                    }
                }
                return true;
            }

            @Override
            public void onPageStarted(WebView view, String url, android.graphics.Bitmap favicon) {
                if (!connecting && trustedOrigin != null &&
                        ServerAddress.isSameOrigin(trustedOrigin, url)) {
                    pageLoadFailed = false;
                }
            }

            @Override
            public void onPageFinished(WebView view, String url) {
                if (!connecting && !pageLoadFailed && screen == Screen.LOADING &&
                        trustedOrigin != null && ServerAddress.isSameOrigin(trustedOrigin, url)) {
                    show(Screen.WEB);
                }
            }

            @Override
            public void onReceivedError(WebView view, WebResourceRequest request,
                                        WebResourceError error) {
                if (request.isForMainFrame() && !connecting &&
                        (screen == Screen.WEB || screen == Screen.LOADING)) {
                    pageLoadFailed = true;
                    showError(getString(R.string.offline_detail));
                }
            }

            @Override
            public void onReceivedHttpError(WebView view, WebResourceRequest request,
                                            WebResourceResponse response) {
                if (request.isForMainFrame() && response.getStatusCode() >= 500 &&
                        !connecting && (screen == Screen.WEB || screen == Screen.LOADING)) {
                    pageLoadFailed = true;
                    showError("The station server returned an error. Try again shortly.");
                }
            }

            @Override
            public void onReceivedSslError(WebView view, SslErrorHandler handler,
                                           SslError error) {
                handler.cancel(); // NEVER allow a certificate exception.
                if (trustedOrigin != null &&
                        ServerAddress.isSameOrigin(trustedOrigin, error.getUrl()) &&
                        !connecting) {
                    pageLoadFailed = true;
                    showError("Secure connection failed. Check the server's HTTPS certificate.");
                }
            }
        });
        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public boolean onShowFileChooser(WebView view, ValueCallback<Uri[]> callback,
                                             FileChooserParams params) {
                if (fileCallback != null) fileCallback.onReceiveValue(null);
                fileCallback = callback;
                try {
                    startActivityForResult(params.createIntent(), FILE_CHOOSER_CODE);
                    return true;
                } catch (Exception e) {
                    fileCallback = null;
                    callback.onReceiveValue(null);
                    Toast.makeText(MainActivity.this, "File picker unavailable.", Toast.LENGTH_SHORT).show();
                    return true;
                }
            }

            @Override
            public void onPermissionRequest(PermissionRequest request) {
                request.deny(); // No camera, mic or other privileged WebView permissions.
            }
        });
        webView.setDownloadListener(this::handleDownload);
    }

    private void showOptions(View anchor) {
        PopupMenu menu = new PopupMenu(this, anchor);
        menu.getMenu().add(0, 1, 0, getString(R.string.reload));
        menu.getMenu().add(0, 2, 1, getString(R.string.connection_settings));
        menu.setOnMenuItemClickListener(item -> {
            if (item.getItemId() == 1) {
                if (trustedOrigin == null || screen != Screen.WEB) {
                    String saved = getPreferences(MODE_PRIVATE).getString(KEY_SERVER, "");
                    if (saved.isEmpty()) showSetup();
                    else verifyAndOpen(saved);
                } else {
                    webView.reload();
                }
                return true;
            }
            if (item.getItemId() == 2) {
                showSetup();
                return true;
            }
            return false;
        });
        menu.show();
    }

    private void verifyAndOpen(String userInput) {
        final String nextOrigin;
        try {
            nextOrigin = ServerAddress.normalize(userInput);
        } catch (IllegalArgumentException e) {
            serverField.setError(e.getMessage());
            showSetup();
            return;
        }
        int sequence = ++connectionSequence;
        connecting = true;
        loadingMessage.setText(R.string.checking_server);
        show(Screen.LOADING);
        verifier.execute(() -> {
            try {
                ConnectionVerifier.verify(nextOrigin);
                runOnUiThread(() -> {
                    if (sequence != connectionSequence || isFinishing() || isDestroyed()) return;
                    boolean changed = trustedOrigin != null && !nextOrigin.equals(trustedOrigin);
                    trustedOrigin = nextOrigin;
                    getPreferences(MODE_PRIVATE).edit().putString(KEY_SERVER, trustedOrigin).apply();
                    connecting = false;
                    loadingMessage.setText(R.string.opening_workspace);
                    pageLoadFailed = false;
                    show(Screen.LOADING);
                    if (changed) {
                        webView.stopLoading();
                        webView.loadUrl("about:blank");
                        webView.clearHistory();
                        CookieManager.getInstance().removeAllCookies(done -> {
                            if (!isFinishing() && !isDestroyed() && sequence == connectionSequence) {
                                openWorkspace();
                            }
                        });
                    } else {
                        openWorkspace();
                    }
                });
            } catch (Exception e) {
                runOnUiThread(() -> {
                    if (sequence != connectionSequence || isFinishing() || isDestroyed()) return;
                    connecting = false;
                    showError(getString(R.string.connection_error));
                });
            }
        });
    }

    private void openWorkspace() {
        if (trustedOrigin == null) {
            showSetup();
            return;
        }
        pageLoadFailed = false;
        loadingMessage.setText(R.string.opening_workspace);
        show(Screen.LOADING);
        webView.loadUrl(trustedOrigin + "/");
    }

    private void handleDownload(String url, String userAgent,
                                String contentDisposition, String mimeType, long size) {
        if (trustedOrigin == null || !ServerAddress.isSameOrigin(trustedOrigin, url)) {
            Toast.makeText(this, "Downloads must come from your AirVote server.", Toast.LENGTH_LONG).show();
            return;
        }
        try {
            DownloadManager.Request request = new DownloadManager.Request(Uri.parse(url));
            String guessed = URLUtil.guessFileName(url, contentDisposition, mimeType);
            String filename = guessed.replaceAll("[^a-zA-Z0-9._-]", "_");
            if (filename.isEmpty() || ".".equals(filename) || "..".equals(filename)) filename = "airvote-export.csv";
            request.setTitle(filename);
            request.setDescription("AirVote authorised export");
            request.setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED);
            request.setMimeType(mimeType == null ? "application/octet-stream" : mimeType);
            if (userAgent != null) request.addRequestHeader("User-Agent", userAgent);
            String cookie = CookieManager.getInstance().getCookie(url);
            if (cookie != null && !cookie.isEmpty()) request.addRequestHeader("Cookie", cookie);
            DownloadManager manager = (DownloadManager) getSystemService(Context.DOWNLOAD_SERVICE);
            manager.enqueue(request);
            Toast.makeText(this, "AirVote download started.", Toast.LENGTH_SHORT).show();
        } catch (Exception e) {
            Toast.makeText(this, "The download could not be started.", Toast.LENGTH_LONG).show();
        }
    }

    private void showSetup() {
        ++connectionSequence;
        connecting = false;
        String saved = getPreferences(MODE_PRIVATE).getString(KEY_SERVER, "");
        serverField.setText(saved);
        show(Screen.SETUP);
    }

    private void showError(String message) {
        errorMessage.setText(message);
        show(Screen.ERROR);
    }

    private void show(Screen next) {
        screen = next;
        setupPanel.setVisibility(next == Screen.SETUP ? View.VISIBLE : View.GONE);
        loadingPanel.setVisibility(next == Screen.LOADING ? View.VISIBLE : View.GONE);
        errorPanel.setVisibility(next == Screen.ERROR ? View.VISIBLE : View.GONE);
        webView.setVisibility(next == Screen.WEB ? View.VISIBLE : View.GONE);
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == FILE_CHOOSER_CODE && fileCallback != null) {
            fileCallback.onReceiveValue(WebChromeClient.FileChooserParams.parseResult(resultCode, data));
            fileCallback = null;
        }
    }

    @Override
    public void onBackPressed() {
        if (screen == Screen.WEB && webView.canGoBack()) {
            int previous = webView.copyBackForwardList().getCurrentIndex() - 1;
            String previousUrl = previous >= 0
                    ? webView.copyBackForwardList().getItemAtIndex(previous).getUrl() : "";
            if (trustedOrigin != null && ServerAddress.isSameOrigin(trustedOrigin, previousUrl)) {
                webView.goBack();
                return;
            }
        }
        if (screen == Screen.ERROR) {
            showSetup();
            return;
        }
        super.onBackPressed();
    }

    @Override
    protected void onDestroy() {
        ++connectionSequence;
        if (fileCallback != null) {
            fileCallback.onReceiveValue(null);
            fileCallback = null;
        }
        verifier.shutdownNow();
        webView.stopLoading();
        webView.destroy();
        super.onDestroy();
    }
}
