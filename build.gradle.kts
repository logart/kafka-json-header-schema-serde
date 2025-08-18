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

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.26.3")
}

tasks {
    withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
    }
    test {
        useJUnitPlatform()
    }
}
