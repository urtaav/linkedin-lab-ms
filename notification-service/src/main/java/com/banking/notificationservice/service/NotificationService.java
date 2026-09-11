package com.banking.notificationservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final EmailService emailService;
    private static final String TEMPLATE = "email/notification";
    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm");

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();
            String senderEmail = (String) payload.get("senderEmail");
            String receiverEmail = (String) payload.get("receiverEmail");

            if (senderEmail != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "DEBIT");
                vars.put("title", "Transaccion realizada");
                vars.put("amount", "-$" + amount);
                vars.put("senderAccount", senderAccount);
                vars.put("receiverAccount", receiverAccount);
                vars.put("date", LocalDateTime.now().format(FMT));
                vars.put("message", "Se ha realizado una transferencia desde tu cuenta.");
                emailService.sendEmail(senderEmail,
                        "DigitalBank - Transaccion realizada", TEMPLATE, vars);
            }

            if (receiverEmail != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "CREDIT");
                vars.put("title", "Deposito recibido");
                vars.put("amount", "+$" + amount);
                vars.put("senderAccount", senderAccount);
                vars.put("receiverAccount", receiverAccount);
                vars.put("date", LocalDateTime.now().format(FMT));
                vars.put("message", "Has recibido una transferencia en tu cuenta.");
                emailService.sendEmail(receiverEmail,
                        "DigitalBank - Deposito recibido", TEMPLATE, vars);
            }

        } catch (Exception e) {
            log.error("Error sending transaction notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");
            String email = (String) payload.get("email");

            if (email != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "FRAUD");
                vars.put("title", "Tu cuenta ha sido bloqueada");
                vars.put("accountNumber", accountNumber);
                vars.put("date", LocalDateTime.now().format(FMT));
                vars.put("reason", reason);
                vars.put("message",
                        "Tu cuenta ha sido bloqueada por seguridad. Contacta a soporte inmediatamente.");
                emailService.sendEmail(email,
                        "DigitalBank - Cuenta bloqueada", TEMPLATE, vars);
            }

        } catch (Exception e) {
            log.error("Error sending fraud alert: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");
            String email = (String) payload.get("email");

            if (email != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "OTP");
                vars.put("title", "Verificacion de seguridad requerida");
                vars.put("accountNumber", accountNumber);
                vars.put("amount", "$" + amount);
                vars.put("otp", otp);
                vars.put("date", LocalDateTime.now().format(FMT));
                vars.put("reason", reason);
                vars.put("message",
                        "Se detecto actividad sospechosa. Confirma tu transaccion con el codigo OTP.");
                emailService.sendEmail(email,
                        "DigitalBank - Verificacion requerida", TEMPLATE, vars);
            }

        } catch (Exception e) {
            log.error("Error sending OTP notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(
            @Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");
            String email = (String) payload.get("email");

            if (email != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "REFUND");
                vars.put("title", "Reembolso procesado");
                vars.put("amount", "+$" + amount);
                vars.put("senderAccount", senderAccount);
                vars.put("date", LocalDateTime.now().format(FMT));
                vars.put("reason", reason);
                vars.put("message",
                        "Tu transaccion fue cancelada. El monto ha sido reembolsado a tu cuenta.");
                emailService.sendEmail(email,
                        "DigitalBank - Reembolso procesado", TEMPLATE, vars);
            }

        } catch (Exception e) {
            log.error("Error sending refund notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String razorpayId = (String) payload.get("razorpayPaymentId");
            String email = (String) payload.get("email");

            if (email != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "PAYMENT_OK");
                vars.put("title", "Pago completado");
                vars.put("amount", "$" + amount);
                vars.put("accountNumber", accountNumber);
                vars.put("date", LocalDateTime.now().format(FMT));
                vars.put("reference", razorpayId);
                vars.put("message", "Tu pago ha sido procesado exitosamente.");
                emailService.sendEmail(email,
                        "DigitalBank - Pago exitoso", TEMPLATE, vars);
            }

        } catch (Exception e) {
            log.error("Error sending payment notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String email = (String) payload.get("email");

            if (email != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "PAYMENT_FAIL");
                vars.put("title", "Pago no procesado");
                vars.put("amount", "$" + amount);
                vars.put("accountNumber", accountNumber);
                vars.put("date", LocalDateTime.now().format(FMT));
                vars.put("reason", "Pago fallido via Razorpay");
                vars.put("message",
                        "Tu pago no pudo ser procesado. Intenta de nuevo o contacta soporte.");
                emailService.sendEmail(email,
                        "DigitalBank - Pago fallido", TEMPLATE, vars);
            }

        } catch (Exception e) {
            log.error("Error sending payment failure notification: {}", e.getMessage());
        }
    }
}
