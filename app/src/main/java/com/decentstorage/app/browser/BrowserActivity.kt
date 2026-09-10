package com.decentstorage.app.browser

import android.os.Bundle
import android.view.Gravity
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import androidx.activity.ComponentActivity
import com.decentstorage.app.StorageClient
import com.decentstorage.app.network.GossipRegistry
import com.decentstorage.app.network.RelayConfig
import com.decentstorage.app.network.ShardRequestHandler
import com.decentstorage.app.network.webrtc.WebRtcManager
import com.decentstorage.app.network.webrtc.RelayTransport
import com.decentstorage.app.network.webrtc.SignalingClient
import org.json.JSONObject
import java.io.ByteArrayInputStream
import java.io.File
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/**
 * App SEPARADO do node (com.decentstorage.browser, applicationId próprio — ver
 * build.gradle deste módulo). Não hospeda shard de ninguém, não tem wallet/anchor/
 * exoplayer: só entra no mesmo signaling que os nós usam, monta um GossipRegistry de
 * capacidade 0 (nunca aceita `put`, só participa do gossip e baixa via WebRTC) e
 * resolve domínio → manifesto → arquivo inteiramente client-side. Em nenhum momento
 * fala HTTP/DNS normal pra navegar — só WebSocket com o signaling (pra achar peers)
 * e WebRTC direto com eles (pra baixar shards).
 */
class BrowserActivity : ComponentActivity() {

    private lateinit var registry: GossipRegistry
    private lateinit var storageClient: StorageClient
    private lateinit var webView: WebView
    private lateinit var statusText: TextView
    private var currentDomain: String? = null
    private val prefs by lazy { getSharedPreferences("vagalun_browser", MODE_PRIVATE) }
    private val relayFallbackExecutor = Executors.newSingleThreadScheduledExecutor()
    private val RELAY_FALLBACK_DELAY_SECONDS = 12L

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        startPeer()
        buildUi()
    }

    // Sobe só o suficiente pra ser um peer leve: nodeId estável, GossipRegistry sem
    // capacidade (nunca serve shard), signaling + WebRTC pra descobrir peers reais e
    // baixar deles. Nada de ShardServer TCP — este app nunca escuta conexão de
    // ninguém, só disca pra fora.
    private fun startPeer() {
        val nodeId = prefs.getString("nodeId", null) ?: run {
            val generated = "browser-" + SecureRandom().nextInt(1_000_000)
            prefs.edit().putString("nodeId", generated).apply()
            generated
        }

        val reg = GossipRegistry(nodeId, "127.0.0.1", 0, 0L, dataDir = filesDir)
        registry = reg
        reg.start()
        storageClient = StorageClient(reg)

        // Handler local: sempre recusa put/get (capacidade 0, dataDir vazio — não
        // hospeda nada), mas responde gossip normalmente, o que é o único op que
        // este app precisa atender quando é ELE quem recebe um pedido de sync.
        val reqHandler = ShardRequestHandler(nodeId, 0L, File(filesDir, "no-shards"), applicationContext) { payload ->
            reg.handleIncomingGossip(payload)
        }

        thread {
            val signalingUrl = RelayConfig.fetchSignalingUrl()
            if (signalingUrl == null) {
                runOnUiThread { statusText.text = "signaling indisponível agora — sem peers pra consultar ainda" }
                return@thread
            }
            connectSignaling(signalingUrl, nodeId, reg, reqHandler)
        }
    }

    private fun connectSignaling(signalingUrl: String, nodeId: String, reg: GossipRegistry, reqHandler: ShardRequestHandler) {
        val sc = SignalingClient(
            signalingUrl,
            nodeId,
            onSignal = { _, _ -> },
            onStateChange = { connected ->
                runOnUiThread { statusText.text = if (connected) "conectado à rede (${reg.knownPeers().size} peer(s))" else "desconectado do signaling" }
            }
        )

        val mgr = WebRtcManager(
            context = this,
            signalingClient = sc,
            selfNodeId = nodeId,
            requestHandler = reqHandler,
            onTransportReady = { peerId, transport -> reg.attachWanTransport(peerId, transport) },
            onTransportClosed = { peerId -> reg.detachWanTransport(peerId) },
            iceServers = WebRtcManager.defaultIceServers()
        )
        sc.onSignal = { from, payload -> mgr.handleSignal(from, payload) }

        sc.onPeerList = { peerIds ->
            peerIds.filter { it != nodeId }.forEach { peerId ->
                if (nodeId < peerId) {
                    mgr.connectToPeer(peerId)
                    scheduleRelayFallback(peerId, reg, sc)
                }
            }
        }
        sc.onPeerJoined = { peerId ->
            if (peerId != nodeId && nodeId < peerId) {
                mgr.connectToPeer(peerId)
                scheduleRelayFallback(peerId, reg, sc)
            }
        }
        sc.onPeerLeft = { peerId -> mgr.disconnect(peerId); reg.detachWanTransport(peerId) }

        sc.onRelayRequest = { from, requestId, header, payload ->
            val (respHeader, respPayload) = try {
                reqHandler.handle(header, payload)
            } catch (e: Exception) {
                JSONObject().put("ok", false).put("error", e.message ?: "erro") to null
            }
            sc.sendRelayResponse(from, requestId, respHeader, respPayload)
        }

        sc.connect()
    }

    // Mesma lógica do app-node: se o WebRTC direto não abrir em N segundos (NAT/
    // firewall difícil), usa o próprio signaling como relay pra shard/gossip. Sem
    // isso, um peer atrás de NAT restritivo nunca troca nada com o browser.
    private fun scheduleRelayFallback(peerId: String, reg: GossipRegistry, sc: SignalingClient) {
        relayFallbackExecutor.schedule({
            val alreadyConnected = reg.knownPeers().find { it.nodeId == peerId }?.webrtcTransport != null
            if (!alreadyConnected) {
                reg.attachWanTransport(peerId, RelayTransport(peerId, sc))
            }
        }, RELAY_FALLBACK_DELAY_SECONDS, TimeUnit.SECONDS)
    }

    private fun buildUi() {
        val root = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }

        val addressBar = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            setPadding(16, 16, 16, 8)
        }
        val domainField = EditText(this).apply {
            hint = "dominio.vgl"
            layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
        }
        val goButton = Button(this).apply { text = "Ir" }
        addressBar.addView(domainField)
        addressBar.addView(goButton)

        statusText = TextView(this).apply { text = "conectando..."; setPadding(16, 0, 16, 8) }

        webView = WebView(this).apply {
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, 0, 1f)
            settings.javaScriptEnabled = true
            webViewClient = object : WebViewClient() {
                override fun shouldInterceptRequest(view: WebView, request: WebResourceRequest): WebResourceResponse? {
                    val domain = currentDomain ?: return super.shouldInterceptRequest(view, request)
                    val path = request.url.path?.takeIf { it.isNotEmpty() } ?: "/"
                    return resolveAndFetch(domain, path)?.let { (bytes, contentType) ->
                        WebResourceResponse(contentType, "utf-8", ByteArrayInputStream(bytes))
                    }
                }
            }
        }

        root.addView(addressBar)
        root.addView(statusText)
        root.addView(webView)
        setContentView(root)

        goButton.setOnClickListener {
            val domain = domainField.text.toString().trim()
            if (domain.isNotEmpty()) navigateTo(domain)
        }
    }

    private fun navigateTo(domain: String) {
        currentDomain = domain
        val (bytes, contentType) = resolveAndFetch(domain, "/") ?: run {
            statusText.text = "não achei '$domain' no índice ainda (${registry.knownPeers().size} peer(s) conectados — " +
                "gossip pode levar alguns segundos, ou o site nunca foi anunciado)"
            webView.loadData("", "text/plain", "utf-8")
            return
        }
        statusText.text = "servido 100% via P2P — nenhuma requisição HTTP/DNS normal foi feita"
        if (contentType.startsWith("text/html")) {
            webView.loadDataWithBaseURL("https://$domain/", String(bytes, Charsets.UTF_8), contentType, "utf-8", null)
        } else {
            webView.loadData(Base64.getEncoder().encodeToString(bytes), contentType, "base64")
        }
    }

    private fun resolveAndFetch(domain: String, path: String): Pair<ByteArray, String>? {
        val site = registry.getSite(domain) ?: return null
        val route = site.routes.find { it.path == path } ?: site.routes.find { it.path == "/" } ?: return null
        return try {
            val fileKey = Base64.getDecoder().decode(route.fileKeyB64)
            storageClient.downloadFileWithKey(route.fileId, fileKey) to route.contentType
        } catch (e: Exception) {
            null
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        if (::registry.isInitialized) registry.stop()
        relayFallbackExecutor.shutdownNow()
    }
}
