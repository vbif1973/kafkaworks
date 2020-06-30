FROM bellsoft/liberica-openjdk-alpine:11.0.5
ENV APP_FILE spring-boot-with-kafka-0.0.1-SNAPSHOT.jar
ENV APP_HOME /app
EXPOSE 80
COPY spring-boot-with-kafka/target/$APP_FILE $APP_HOME/
WORKDIR $APP_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -jar $APP_FILE"]