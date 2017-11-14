package cn.helium.kvstore.util;

/**
 * Copyright @ 2017 unknown.yuzhouwan.com
 * All right reserved.
 * Function: String Utils
 *
 * @author Grace Koo
 * @since 2017/10/29 0029
 */
public final class StrUtils {

    private StrUtils() {
    }

    public static boolean isEmpty(String s) {
        return s == null || s.length() == 0;
    }
}
