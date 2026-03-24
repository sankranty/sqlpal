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
    implementation(project(":"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("org.postgresql:postgresql:42.7.8")

    testImplementation(kotlin("test"))
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.jar.configure {
    manifest {
        attributes("Main-Class" to "MainKt")
    }
    configurations["compileClasspath"].forEach { from(zipTree(it.absoluteFile)) }
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "11"
}