package com.banking.accountservice.service;

import com.banking.accountservice.dto.AuthResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.dto.LoginRequest;
import com.banking.auth.common.JwtUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuthService {

    private final AccountService accountService;
    private final PasswordEncoder passwordEncoder;

    public AuthResponse login(LoginRequest request) {
        log.info("Login attempt for email: {}", request.getEmail());

        var account = accountService.findByEmail(request.getEmail());

        if (account.isEmpty()) {
            throw new RuntimeException("Invalid email or password");
        }

        if (!passwordEncoder.matches(request.getPassword(), account.get().getPassword())) {
            throw new RuntimeException("Invalid email or password");
        }

        String token = JwtUtil.generateToken(account.get().getEmail(), "ROLE_USER");

        log.info("Login successful for email: {}", request.getEmail());
        return new AuthResponse(token, account.get().getEmail(), "ROLE_USER");
    }

    public AuthResponse register(CreateAccountRequest request) {
        log.info("Registering new user: {}", request.getEmail());

        String encodedPassword = passwordEncoder.encode(request.getPassword());

        var response = accountService.createAccount(request, encodedPassword);

        String token = JwtUtil.generateToken(response.getEmail(), "ROLE_USER");

        log.info("Registration successful for email: {}", request.getEmail());
        return new AuthResponse(token, response.getEmail(), "ROLE_USER");
    }
}
