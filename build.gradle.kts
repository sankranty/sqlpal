import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.10"
    id("io.exoquery.terpal-plugin") version "1.8.21-0.1.0"

    // ----------------- Plugins for publishing --------------------
    // Creates package for publication (incorporates maven-publish and dokka plugins).
    id("org.hildan.kotlin-publish") version "1.7.0"
    // Generates JavaDoc comments (is automatically configured by hildan plugin).
    id("org.jetbrains.dokka") version "1.8.20"
    // Signs package by gpg utility.
    id("signing")
    // Publishes package to maven central. Is used because hildan plugin supports
    // only publication via OSSRH, that is no longer supported by maven central.
    id("tech.yanand.maven-central-publish") version "1.3.0"
}

group = "org.sqlpal"
version = "1.0.4"

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

// Configures signing of package for publication (public key is obtained from gradle.properties file).
signing {
    useGpgCmd() // Is used to prompt for password instead of strong password in gradle.properties file.
    sign(publishing.publications) // Do sign.
}

// Configures publication to maven central portal.
mavenCentral {
    // Set auth token to authorize on maven central portal
    // from 'mavenCentralAuthToken' property specified in gradle.properties file.
    authToken.set(providers.gradleProperty("mavenCentralAuthToken").get())
}

// Defines package metadata.
publishing {
    publications.withType<MavenPublication>().configureEach {
        pom {
            name.set("SqlPal")
            description.set("Ultralight ORM for Kotlin.")
            url.set("https://github.com/sankranty/sqlpal")

            licenses {
                license {
                    name.set("Apache-2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
            developers {
                developer {
                    id.set("sankranti")
                    name.set("Stanislav Antonov")
                    email.set("sankranty@gmail.com")
                    url.set("https://github.com/sankranty")
                }
            }
            scm {
                url.set("https://github.com/sankranty/sqlpal")
                connection.set("scm:git:git://github.com/sankranty/sqlpal.git")
                developerConnection.set("scm:git:ssh://git@github.com/sankranty/sqlpal.git")
            }
        }
    }
}