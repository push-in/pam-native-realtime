package dev.pam.realtime

import android.util.Base64
import dev.pam.nativeapp.HostEventListener
import dev.pam.nativeapp.HostEvents
import dev.pam.nativeapp.modules.ModuleCompletion
import dev.pam.nativeapp.modules.NativeModule
import dev.pam.nativeapp.modules.SurfaceRetirableModule
import dev.pam.nativeapp.protocol.WireMap
import dev.pam.nativeapp.protocol.WireValue
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import okio.ByteString.Companion.toByteString
import org.json.JSONObject
import java.lang.ref.WeakReference
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The sockets belong to the retained PHP runtime, not to the Activity: when a
 * recreated Activity (Android 7 trimming a stopped task, font size, display
 * size or language) gets a new module registry, the retired module hands its
 * live Pusher clients and WebSockets to the module that replaces it. Closing
 * them there left PHP with a client it believed connected and no socket.
 */
class RealtimeModule(context: android.content.Context) : NativeModule, SurfaceRetirableModule {
    private val appContext=context.applicationContext?:context
    @Volatile private var sockets=Sockets()
    @Volatile private var retired=false

    init {
        synchronized(lock){ newest=WeakReference(this) }
        HostEvents.register(foreground)
    }

    /** Back in the foreground: Pusher clients probe a socket that stayed silent (it may be dead server-side). */
    private fun wake(){ if(!retired) sockets.wake() }

    override fun invoke(method:String,payload:ByteArray,completion:ModuleCompletion){val s=sockets;if(method.startsWith("pusher")){runCatching{s.pusher(method,WireMap.decode(payload),completion)}.onFailure{completion.failure(it)};return};runCatching{val values=WireMap.decode(payload);when(method){"connect"->s.connect(values);"send"->s.send(values);"poll"->{s.poll(values,completion);return};"state"->s.state(values);"close"->s.close(values);else->error("Unknown method: $method")}}.onSuccess{completion.success(it)}.onFailure{completion.failure(it)}}

    override fun retireSurface(){
        val successor=synchronized(lock){ if(retired) return; retired=true; newest?.get()?.takeIf{it!==this&&!it.retired} }
        val live=sockets
        if(successor==null||!successor.adopt(live)) live.close()
    }

    override fun close(){
        val live=synchronized(lock){ if(retired) return; retired=true; if(newest?.get()===this) newest=null; sockets }
        live.close()
    }

    /** Takes over a retired module's sockets unless PHP already used this module's own. */
    private fun adopt(previous:Sockets):Boolean{
        val own=synchronized(lock){ if(retired) return false; sockets.takeIf{it.idle()}?.also{sockets=previous} } ?: return false
        own.close()
        return true
    }

    private inner class Sockets {
        private val client=OkHttpClient.Builder().pingInterval(25,TimeUnit.SECONDS).build();private val pusher=ConcurrentHashMap<String,PusherClient>();private val scheduler=Executors.newSingleThreadScheduledExecutor();private val connections=ConcurrentHashMap<String,Connection>()
        fun idle()=pusher.isEmpty()&&connections.isEmpty()
        fun wake()=pusher.values.forEach{it.wake()}
        fun connect(v:Map<String,WireValue>):Map<String,WireValue>{val id=UUID.randomUUID().toString();val connection=Connection(id,v.integer("maxMessageBytes").toInt());connections[id]=connection;val builder=Request.Builder().url(v.text("url"));val headers=JSONObject(v.text("headers"));headers.keys().forEach{key->builder.header(key,headers.getString(key))};v.text("protocols").takeIf{it.isNotEmpty()}?.let{builder.header("Sec-WebSocket-Protocol",it)};connection.socket=client.newWebSocket(builder.build(),connection);return mapOf("identifier" to WireValue.Text(id))}
        fun send(v:Map<String,WireValue>):Map<String,WireValue>{val c=connection(v);val payload=v.text("payload");val sent=if(v.flag("binary")){val bytes=Base64.decode(payload,Base64.DEFAULT);require(bytes.size<=c.maxMessageBytes){"Message exceeds connection limit"};c.socket?.send(bytes.toByteString())==true}else{require(payload.toByteArray().size<=c.maxMessageBytes){"Message exceeds connection limit"};c.socket?.send(payload)==true};check(sent){"WebSocket rejected message"};return emptyMap()}
        fun poll(v:Map<String,WireValue>,completion:ModuleCompletion){val c=connection(v);val generation:Int;val ready:Event?;synchronized(c){check(c.waiter==null){"Only one poll may be pending per connection"};ready=c.events.removeFirstOrNull();if(ready==null){c.pollGeneration++;generation=c.pollGeneration;c.waiter=completion}else{generation=c.pollGeneration}};if(ready!=null){completion.success(ready.values());if(ready.kind==4||ready.kind==5)connections.remove(c.id,c);return};val timeout=v.integer("timeoutMillis");scheduler.schedule({val waiter=synchronized(c){if(c.pollGeneration!=generation)null else c.waiter.also{c.waiter=null;c.pollGeneration++}};waiter?.success(Event(7).values())},timeout,TimeUnit.MILLISECONDS)}
        fun state(v:Map<String,WireValue>)=mapOf("state" to WireValue.Integer(connection(v).state.toLong()))
        fun close(v:Map<String,WireValue>):Map<String,WireValue>{val c=connection(v);c.state=3;check(c.socket?.close(v.integer("code").toInt(),v.text("reason"))==true){"WebSocket could not close"};return emptyMap()}
        private fun connection(v:Map<String,WireValue>)=connections[v.text("identifier")]?:error("Unknown realtime connection")
        fun pusher(method:String,v:Map<String,WireValue>,completion:ModuleCompletion){
            val id=v.text("client")
            if(method=="pusherConnect"){
                val config=PusherConfig.parse(v.text("config"))
                val created=PusherClient(id,config,client,appContext)
                pusher.put(id,created)?.close()
                created.start()
                completion.success()
                return
            }
            val target=pusher[id]
            if(target==null){completion.failure("Unknown realtime client $id");return}
            when(method){
                "pusherNext"->target.events.next(completion)
                "pusherSubscribe"->{target.subscribe(v.text("channel"));completion.success()}
                "pusherUnsubscribe"->{target.unsubscribe(v.text("channel"));completion.success()}
                "pusherWhisper"->target.whisper(v.text("channel"),v.text("event"),v.text("data"),completion)
                "pusherToken"->{target.token(v.text("bearer").takeIf{it.isNotEmpty()});completion.success()}
                "pusherReconnect"->{target.reconnect();completion.success()}
                "pusherStatus"->target.status(completion)
                "pusherDisconnect"->{pusher.remove(id,target);target.close();completion.success()}
                else->completion.failure("Unknown method: $method")
            }
        }
        fun close(){pusher.values.forEach{it.close()};pusher.clear();connections.values.forEach{it.socket?.cancel();it.finish(Event(4,code=1001))};connections.clear();scheduler.shutdownNow();client.dispatcher.executorService.shutdown();client.connectionPool.evictAll()}
        private inner class Connection(val id:String,val maxMessageBytes:Int):WebSocketListener(){@Volatile var state=1;@Volatile var socket:WebSocket?=null;val events=ArrayDeque<Event>();var waiter:ModuleCompletion?=null;var pollGeneration=0
            override fun onOpen(webSocket:WebSocket,response:Response){state=2;offer(Event(1))}
            override fun onMessage(webSocket:WebSocket,text:String){if(text.toByteArray().size>maxMessageBytes){webSocket.close(1009,"Message too large");return};offer(Event(2,text))}
            override fun onMessage(webSocket:WebSocket,bytes:ByteString){if(bytes.size>maxMessageBytes){webSocket.close(1009,"Message too large");return};offer(Event(3,Base64.encodeToString(bytes.toByteArray(),Base64.NO_WRAP)))}
            override fun onClosing(webSocket:WebSocket,code:Int,reason:String){state=3}
            override fun onClosed(webSocket:WebSocket,code:Int,reason:String){state=4;finish(Event(4,reason,code))}
            override fun onFailure(webSocket:WebSocket,t:Throwable,response:Response?){state=5;finish(Event(5,t.message.orEmpty(),response?.code?:0))}
            fun offer(event:Event){val pending:ModuleCompletion?;synchronized(this){pending=waiter;if(pending!=null){waiter=null;pollGeneration++}else{if(events.size>=256)events.removeFirst();events.addLast(event)}};pending?.success(event.values());if(pending!=null&&(event.kind==4||event.kind==5))connections.remove(id,this)}
            fun finish(event:Event){offer(event)}
        }
    }
    private data class Event(val kind:Int,val payload:String="",val code:Int=0){fun values()=mapOf("kind" to WireValue.Integer(kind.toLong()),"payload" to WireValue.Text(payload),"code" to WireValue.Integer(code.toLong()))}
    private fun ModuleCompletion.failure(error:Throwable)=failure(error.message?:"Realtime failure")

    private companion object {
        val lock=Any()
        /** The module of the newest registry: the one a retired module hands its sockets to. */
        var newest:WeakReference<RealtimeModule>?=null

        /** Process-wide and registered once (HostEvents keeps listeners for the whole process). */
        val foreground=object:HostEventListener{
            override fun foreground(active:Boolean){ if(active) synchronized(lock){ newest?.get() }?.wake() }
        }
    }
}
