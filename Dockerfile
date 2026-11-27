# Stage 1: Build the Spring Boot application
FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /app

# Copy Maven configuration and source code
COPY pom.xml .
COPY src ./src

# Build the application and run tests
RUN mvn -B clean package

# Stage 2: Run the application
FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy the generated JAR from the build stage
COPY --from=build /app/target/upi-offline-mesh-0.0.1-SNAPSHOT.jar app.jar

# Application port
EXPOSE 8080

# Start Spring Boot
ENTRYPOINT ["java", "-jar", "app.jar"]