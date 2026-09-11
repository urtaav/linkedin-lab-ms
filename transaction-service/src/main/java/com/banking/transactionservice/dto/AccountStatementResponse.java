package com.banking.transactionservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AccountStatementResponse {

    private String accountNumber;
    private String accountHolderName;
    private String email;
    private String accountType;
    private BigDecimal currentBalance;
    private BigDecimal totalDebited;
    private BigDecimal totalCredited;
    private int totalTransactions;
    private int completedTransactions;
    private int flaggedTransactions;
    private LocalDateTime statementDate;
    private List<TransactionResponse> transactions;
}
