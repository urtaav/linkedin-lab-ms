package com.banking.paymentservice.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @CircuitBreaker(name = "accountService", fallbackMethod = "emailFallback")
    @Retry(name = "accountService")
    @GetMapping("/api/v1/accounts/{accountNumber}/email")
    String getEmail(@PathVariable String accountNumber);

    default String emailFallback(String accountNumber, Throwable t) {
        return null;
    }
}
