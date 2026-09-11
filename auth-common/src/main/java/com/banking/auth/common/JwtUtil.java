package com.banking.auth.common;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import javax.crypto.SecretKey;
import java.util.Date;

public class JwtUtil {

    private static final SecretKey key = Keys.hmacShaKeyFor(
            AuthConstants.SECRET.getBytes());

    private JwtUtil() {}

    public static String generateToken(String email, String roles) {
        return Jwts.builder()
                .subject(email)
                .claim("roles", roles)
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + AuthConstants.EXPIRATION))
                .signWith(key)
                .compact();
    }

    public static Claims parseToken(String token) {
        return Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    public static String getEmail(String token) {
        return parseToken(token).getSubject();
    }

    public static String getRoles(String token) {
        return parseToken(token).get("roles", String.class);
    }

    public static boolean isValid(String token) {
        try {
            parseToken(token);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
