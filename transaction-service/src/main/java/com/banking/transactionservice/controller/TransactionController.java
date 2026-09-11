package com.banking.transactionservice.controller;

import com.banking.transactionservice.dto.AccountStatementResponse;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.service.TransactionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Transactions", description = "Transaction management endpoints")
public class TransactionController {

    private final TransactionService transactionService;

    @Operation(summary = "Transfer money between accounts")
    @PostMapping("/transfer")
    public ResponseEntity<TransactionResponse> transfer(
            @Valid @RequestBody TransferRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(transactionService.transfer(request));
    }

    @Operation(summary = "Get transaction by ID")
    @GetMapping("/{transactionId}")
    public ResponseEntity<TransactionResponse> getTransaction(
            @PathVariable String transactionId) {
        return ResponseEntity.ok(
                transactionService.getTransaction(transactionId));
    }

    @Operation(summary = "Get transaction history for an account")
    @GetMapping("/account/{accountNumber}")
    public ResponseEntity<List<TransactionResponse>> getHistory(
            @PathVariable String accountNumber) {
        return ResponseEntity.ok(
                transactionService.getTransactionHistory(accountNumber));
    }

    @Operation(summary = "Verify OTP for a transaction")
    @PostMapping("/{transactionId}/verify")
    public ResponseEntity<TransactionResponse> verifyTransaction(
            @PathVariable String transactionId,
            @RequestParam String otp) {
        log.info("OTP verification request — transaction: {}",
                transactionId);
        return ResponseEntity.ok(
                transactionService.verifyOTP(transactionId, otp));
    }

    @Operation(summary = "Get full account statement with summary")
    @GetMapping("/account/{accountNumber}/statement")
    public ResponseEntity<AccountStatementResponse> getAccountStatement(
            @PathVariable String accountNumber) {
        return ResponseEntity.ok(
                transactionService.getAccountStatement(accountNumber));
    }
}
