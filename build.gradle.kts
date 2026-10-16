plugins {
    id("com.android.library") version "8.9.2"
    id("org.jetbrains.kotlin.android") version "2.1.20"
    id("org.jetbrains.kotlin.jvm") version "2.1.20" apply false
    id("org.jetbrains.kotlin.plugin.serialization") version "2.1.20"
    id("org.jetbrains.kotlin.plugin.compose") version "2.1.20" apply false
    id("com.android.application") version "8.9.2" apply false
    id("org.jetbrains.kotlinx.kover") version "0.9.1" apply false
    `maven-publish`
}
group = "com.github.henriquealbert"
version = "0.1.2"
android {
    namespace = "io.github.henriquealbert.transmit"
    compileSdk = 35
    defaultConfig { minSdk = 24 }
    compileOptions { sourceCompatibility = JavaVersion.VERSION_17; targetCompatibility = JavaVersion.VERSION_17 }
    publishing { singleVariant("release") { withSourcesJar() } }
}
kotlin { jvmToolchain(17) }
dependencies {
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")
    api("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.squareup.okhttp3:okhttp-sse:4.12.0")
}
afterEvaluate {
    publishing.publications.create<MavenPublication>("release") {
        from(components["release"])
        artifactId = "transmit-kotlin"
        pom {
            name.set("transmit-kotlin")
            description.set("Kotlin and Android client for AdonisJS Transmit")
            url.set("https://github.com/henriquealbert/transmit-kotlin")
            licenses { license { name.set("MIT"); url.set("https://opensource.org/license/mit") } }
            developers { developer { name.set("Henrique Albert Schmaiske"); email.set("henrique@elcubo.io") } }
            scm { url.set("https://github.com/henriquealbert/transmit-kotlin") }
        }
    }
}
