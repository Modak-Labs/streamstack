package io.streamstack.server;

import java.util.Objects;
import java.util.Locale;

public final class ContentTypes {

    private ContentTypes() {
    }

    public static boolean isJson(String mime) {
        return "application/json".equals(mime) || mime.endsWith("+json");
    }

    public static String mimeOf(String contentType) {
        if (Objects.isNull(contentType)) {
            return "";
        }
        int semi = contentType.indexOf(';');
        return (semi >= 0 ? contentType.substring(0, semi) : contentType).trim().toLowerCase(Locale.ROOT);
    }

    public static boolean mimeEquals(String a, String b) {
        return mimeOf(a).equals(mimeOf(b));
    }
}
