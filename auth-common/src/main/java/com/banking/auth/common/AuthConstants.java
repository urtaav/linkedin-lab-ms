package com.banking.auth.common;

public class AuthConstants {
    public static final String SECRET = "404E635266556A586E3272357538782F413F4428472B4B6250645367566B5970";
    public static final long EXPIRATION = 86400000; // 24 hours
    public static final String TOKEN_PREFIX = "Bearer ";
    public static final String HEADER = "Authorization";
    public static final String HEADER_USER_EMAIL = "X-User-Email";
    public static final String HEADER_USER_ROLES = "X-User-Roles";

    private AuthConstants() {}
}
