# 멀티스테이지 빌드
FROM gradle:8.4-jdk21 AS build

WORKDIR /app

# gradle 파일들 복사
COPY build.gradle.kts settings.gradle.kts ./
COPY gradle gradle
COPY gradlew ./

# 소스코드 복사
COPY common/ common/
COPY consumer/ consumer/

# gradle 빌드
RUN ./gradlew :consumer:bootJar

# 런타임
FROM eclipse-temurin:21-jre

WORKDIR /app

# 빌드된 jar 복사
COPY --from=build /app/consumer/build/libs/consumer-1.0-SNAPSHOT.jar app.jar

# 포트 노출
EXPOSE 52002

# 애플리케이션 실행
ENTRYPOINT ["java", "-jar", "app.jar"]