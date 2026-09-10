package com.decentstorage.app.network

import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import java.util.concurrent.TimeUnit

// Cópia do RelayConfig do app-node (MainActivity.kt) — mesmo endereço, porque o
// navegador precisa achar o MESMO signaling server que os nós usam pra aparecer no
// onPeerList. Isolado aqui porque este é um módulo/app separado do node.
object RelayConfig {
    private const val CONFIG_URL =
        "https://raw.githubusercontent.com/riquelima805/adla-nft-market/refs/heads/main/reley.json"

    private val client = OkHttpClient.Builder()
        .connectTimeout(8, TimeUnit.SECONDS)
        .readTimeout(8, TimeUnit.SECONDS)
        .build()

    fun fetchSignalingUrl(): String? {
        return try {
            val request = Request.Builder().url(CONFIG_URL).build()
            client.newCall(request).execute().use { resp ->
                if (!resp.isSuccessful) return null
                val body = resp.body?.string()?.trim()
                if (body.isNullOrBlank()) return null
                val json = JSONObject(body)
                val url = json.optString("signalingUrl", json.optString("url", json.optString("wss", "")))
                url.trim().ifBlank { null }
            }
        } catch (e: Exception) {
            null
        }
    }
}
