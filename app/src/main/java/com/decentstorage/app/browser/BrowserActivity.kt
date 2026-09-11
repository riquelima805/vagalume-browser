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
import com.decentstorage.app.network.NodeIdentity
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
        // Desde o patch anti-hijack de nodeId no signaling, TODO register de
        // peer não-infra precisa provar posse via assinatura Ed25519
        // (pubkey+sig) — sem isso o servidor responde 'register_unauthorized'
        // e o app fica pra sempre com 0 peers, sem nenhum aviso. A identidade
        // aqui é local ao app (gerada uma vez, guardada em SharedPreferences),
        // não é a wallet do usuário — só prova "sou sempre o mesmo dono deste
        // nodeId entre sessões", que é tudo que o signaling exige.
        val identity = NodeIdentity.load(applicationContext)

        val sc = SignalingClient(
            signalingUrl,
            nodeId,
            onSignal = { _, _ -> },
            onStateChange = { connected ->
                runOnUiThread { statusText.text = if (connected) "conectado à rede (${reg.knownPeers().size} peer(s))" else "desconectado do signaling" }
            },
            walletPubkeyBase58 = identity.pubkeyBase58,
            signNodeId = identity.sign
        )
        sc.onError = { reason, detail ->
            runOnUiThread {
                statusText.text = "erro do signaling: $reason" + (detail?.let { " — $it" } ?: "")
            }
        }

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

        // O navegador NUNCA precisa esperar ser chamado — ele só consome
        // (capacidade 0, nunca serve nada pra ninguém). A checagem "só
        // inicia se meu nodeId < peerId" existe pra evitar dois NÓS REAIS
        // discando um pro outro ao mesmo tempo (glare no WebRTC), mas como
        // nodeId aqui sempre começa com "browser-" (baixo na ordem
        // alfabética), essa checagem podia fazer o navegador nunca discar
        // pro node — e o node, por sua vez, nunca disca pro navegador (só
        // se importa com outros nós reais). Resultado: WebRTC nunca
        // conectava, o navegador nunca recebia gossip nenhum, e por isso
        // nenhum site aparecia nunca, mesmo com o signaling funcionando.
        sc.onPeerList = { peerIds ->
            peerIds.filter { it != nodeId }.forEach { peerId ->
                mgr.connectToPeer(peerId)
                scheduleRelayFallback(peerId, reg, sc)
            }
        }
        sc.onPeerJoined = { peerId ->
            if (peerId != nodeId) {
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
        val debugButton = Button(this).apply { text = "🔍" }
        addressBar.addView(domainField)
        addressBar.addView(goButton)
        addressBar.addView(debugButton)

        statusText = TextView(this).apply { text = "conectando..."; setPadding(16, 0, 16, 8) }

        webView = WebView(this).apply {
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, 0, 1f)
            settings.javaScriptEnabled = true
            webViewClient = object : WebViewClient() {
                override fun shouldInterceptRequest(view: WebView, request: WebResourceRequest): WebResourceResponse? {
                    val domain = currentDomain ?: return super.shouldInterceptRequest(view, request)
                    val path = request.url.path?.takeIf { it.isNotEmpty() } ?: "/"
                    val result = resolveAndFetch(domain, path)
                    if (result !is FetchResult.Success) return null
                    return WebResourceResponse(result.contentType, "utf-8", ByteArrayInputStream(result.bytes))
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

        debugButton.setOnClickListener { showDebugInfo() }
    }

    // Painel de diagnóstico: mostra exatamente o que o registry sabe agora —
    // quem são os peers (e se cada um tem transporte WebRTC ativo ou só
    // relay/nenhum) e quais domínios já foram aprendidos via gossip. Sem
    // isso, "não achei o site" é uma caixa preta — com isso dá pra saber
    // na hora se é falta de peer, peer sem WebRTC de verdade, ou o domínio
    // realmente nunca chegou.
    private fun showDebugInfo() {
        val peersInfo = registry.knownPeers().joinToString("\n") { p ->
            val transporte = when {
                p.webrtcTransport != null -> "webrtc ✅"
                else -> "sem transporte ❌ (só apareceu via gossip de outro peer, nunca conectou direto)"
            }
            "• ${p.nodeId} — alive=${p.alive} — $transporte"
        }.ifEmpty { "(nenhum peer conhecido)" }

        val sitesInfo = registry.listSites().joinToString("\n") { "• $it" }.ifEmpty { "(nenhum site conhecido ainda)" }

        val msg = "PEERS (${registry.knownPeers().size}):\n$peersInfo\n\nSITES CONHECIDOS (${registry.listSites().size}):\n$sitesInfo"

        android.app.AlertDialog.Builder(this)
            .setTitle("Debug — estado do gossip")
            .setMessage(msg)
            .setPositiveButton("Fechar", null)
            .show()
    }

    private fun navigateTo(domain: String) {
        currentDomain = domain
        val result = resolveAndFetch(domain, "/")
        if (result is FetchResult.Failure) {
            statusText.text = result.reason
            webView.loadData("", "text/plain", "utf-8")
            return
        }
        result as FetchResult.Success
        statusText.text = "servido 100% via P2P — nenhuma requisição HTTP/DNS normal foi feita"
        if (result.contentType.startsWith("text/html")) {
            webView.loadDataWithBaseURL("https://$domain/", String(result.bytes, Charsets.UTF_8), result.contentType, "utf-8", null)
        } else {
            webView.loadData(Base64.getEncoder().encodeToString(result.bytes), result.contentType, "base64")
        }
    }

    // Antes as 3 causas de falha (site não existe no índice / rota não existe
    // no manifesto / download do shard falhou) viravam a MESMA mensagem
    // genérica "não achei" — impossível saber qual das três era sem debugar
    // o app. Agora cada uma fala exatamente o que aconteceu.
    private sealed class FetchResult {
        data class Success(val bytes: ByteArray, val contentType: String) : FetchResult()
        data class Failure(val reason: String) : FetchResult()
    }

    private fun resolveAndFetch(domain: String, path: String): FetchResult {
        val site = registry.getSite(domain)
            ?: return FetchResult.Failure(
                "não achei '$domain' no índice (${registry.knownPeers().size} peer(s) conectados — " +
                    "gossip pode levar alguns segundos, ou o site nunca foi anunciado/o domínio está diferente do publicado)"
            )

        val route = site.routes.find { it.path == path } ?: site.routes.find { it.path == "/" }
            ?: return FetchResult.Failure("achei o site '$domain', mas o manifesto não tem nenhuma rota pra '$path' nem '/'")

        return try {
            val fileKey = Base64.getDecoder().decode(route.fileKeyB64)
            val bytes = storageClient.downloadFileWithKey(route.fileId, fileKey)
            FetchResult.Success(bytes, route.contentType)
        } catch (e: Exception) {
            FetchResult.Failure(
                "achei o manifesto de '$domain', mas falhou baixar o arquivo (fileId=${route.fileId.take(8)}...): " +
                    "${e.javaClass.simpleName}: ${e.message} — provavelmente nenhum peer conectado agora tem os shards " +
                    "desse arquivo (k mínimo de blocos não disponível)"
            )
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        if (::registry.isInitialized) registry.stop()
        relayFallbackExecutor.shutdownNow()
    }
}
