package com.hmdp.utils;

public class SystemConstants {
    public static final String IMAGE_UPLOAD_DIR = System.getenv("HMDP_IMAGE_UPLOAD_DIR") != null
            ? System.getenv("HMDP_IMAGE_UPLOAD_DIR")
            : java.nio.file.Paths.get("frontend", "public", "imgs").toAbsolutePath().toString();
    public static final String USER_NICK_NAME_PREFIX = "user_";
    public static final int DEFAULT_PAGE_SIZE = 5;
    public static final int MAX_PAGE_SIZE = 10;
}
