package com.banking.transactionservice.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;
import java.util.Map;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @CircuitBreaker(name = "accountService", fallbackMethod = "deductFallback")
    @Retry(name = "accountService")
    @PutMapping("/api/v1/accounts/{accountNumber}/deduct")
    String deductBalance(@PathVariable String accountNumber,
                         @RequestParam BigDecimal amount);

    @CircuitBreaker(name = "accountService", fallbackMethod = "creditFallback")
    @Retry(name = "accountService")
    @PutMapping("/api/v1/accounts/{accountNumber}/credit")
    String creditBalance(@PathVariable String accountNumber,
                         @RequestParam BigDecimal amount);

    @CircuitBreaker(name = "accountService", fallbackMethod = "balanceFallback")
    @Retry(name = "accountService")
    @GetMapping("/api/v1/accounts/{accountNumber}/balance")
    BigDecimal getBalance(@PathVariable String accountNumber);

    @CircuitBreaker(name = "accountService", fallbackMethod = "emailFallback")
    @Retry(name = "accountService")
    @GetMapping("/api/v1/accounts/{accountNumber}/email")
    String getEmail(@PathVariable String accountNumber);

    @CircuitBreaker(name = "accountService", fallbackMethod = "accountFallback")
    @Retry(name = "accountService")
    @GetMapping("/api/v1/accounts/{accountNumber}")
    Map<String, Object> getAccount(@PathVariable String accountNumber);

    default String deductFallback(String accountNumber, BigDecimal amount, Throwable t) {
        throw new RuntimeException("Account Service unavailable - deduct failed: " + t.getMessage());
    }

    default String creditFallback(String accountNumber, BigDecimal amount, Throwable t) {
        throw new RuntimeException("Account Service unavailable - credit failed: " + t.getMessage());
    }

    default BigDecimal balanceFallback(String accountNumber, Throwable t) {
        throw new RuntimeException("Account Service unavailable - balance check failed: " + t.getMessage());
    }

    default String emailFallback(String accountNumber, Throwable t) {
        return null;
    }

    default Map<String, Object> accountFallback(String accountNumber, Throwable t) {
        return Map.of(
            "accountHolderName", "N/A",
            "email", "N/A",
            "accountType", "N/A",
            "balance", BigDecimal.ZERO
        );
    }
}
