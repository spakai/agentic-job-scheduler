FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -B -ntp -DskipTests package dependency:copy-dependencies \
    -DincludeScope=runtime -DoutputDirectory=target/dependency

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN groupadd --system app && useradd --system --gid app --home-dir /app app
COPY --from=build /workspace/target/agentic-job-scheduler-0.1.0-SNAPSHOT.jar /app/app.jar
COPY --from=build /workspace/target/dependency /app/dependency
USER app
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/dependency/*", "com.example.agenticjobscheduler.runtime.WorkerMain"]
