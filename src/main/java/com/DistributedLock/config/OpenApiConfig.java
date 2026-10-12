package com.DistributedLock.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
//Swagger API documentation configuration.
//http://localhost:8080/swagger-ui.html
    @Bean
    public OpenAPI distributedLockOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Distributed Locking Service API")
                        .description("High-concurrency distributed locking system using Redis Lua scripts, FIFO queuing, and Deadlock Detection.")
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Distributed Locking Team")
                                .email("admin@distributedlock.com")));
    }
}
