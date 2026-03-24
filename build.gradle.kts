import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.10"
    id("io.exoquery.terpal-plugin") version "1.8.21-0.1.0"
}

group = "org.sqlpal"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    api("io.exoquery:terpal-runtime:1.0.6")
    api ("com.zaxxer:HikariCP:7.0.2")
    testImplementation(kotlin("test"))
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
}