package com.banking.frauddetectionservice.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.math.BigDecimal;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @CircuitBreaker(name = "accountService", fallbackMethod = "balanceFallback")
    @Retry(name = "accountService")
    @GetMapping("/api/v1/accounts/{accountNumber}/balance")
    BigDecimal getBalance(@PathVariable String accountNumber);

    default BigDecimal balanceFallback(String accountNumber, Throwable t) {
        throw new RuntimeException("Account Service unavailable - balance check failed: " + t.getMessage());
    }
}
