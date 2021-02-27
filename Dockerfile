#FROM adoptopenjdk:11-jre-hotspot
FROM adoptopenjdk/openjdk11:x86_64-alpine-jre-11.0.10_9
MAINTAINER firat_sezer
USER root:root
ENV TIMEZONE=Europe/Istanbul
RUN echo ${TIMEZONE} > /etc/timezone
ARG JAR_FILE=target/*.jar
WORKDIR /home/
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/home/app.jar"]