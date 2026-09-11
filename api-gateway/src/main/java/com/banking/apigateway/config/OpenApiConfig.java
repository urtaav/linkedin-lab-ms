package com.banking.apigateway.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
            .info(new Info()
                .title("API Gateway")
                .version("1.0")
                .description("Digital Banking - API Gateway (routes to Account, Transaction, Payment services)")
                .contact(new Contact().name("DigitalBank")));
    }
}
