FROM eclipse-temurin:17-jdk-focal as builder
WORKDIR /app
COPY *.java ./
RUN javac HelloWorld.java

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/*.class ./
EXPOSE 8080
CMD ["java", "HelloWorld"]
