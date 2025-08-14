plugins {
    id("java")
    id("maven-publish")
}

group = "com.github.logart"
version = "1.5-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

val kafkaVersion = "3.6.0"
val confluentVersion = "7.5.9"

dependencies {
    compileOnly("org.apache.kafka:connect-api:${kafkaVersion}")  // provided scope in Maven

    implementation("io.confluent:kafka-json-schema-serializer:${confluentVersion}")
    implementation("io.confluent:kafka-connect-json-schema-converter:${confluentVersion}")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}
