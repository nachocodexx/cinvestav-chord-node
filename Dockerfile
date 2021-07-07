FROM openjdk:8-alpine
COPY ./target/scala-2.13/chordnode.jar /app/src/app.jar
WORKDIR /app/src
ENTRYPOINT ["java", "-jar","/app/src/app.jar"]
