package dev.pam.realtime

import android.util.Base64
import dev.pam.nativeapp.modules.ModuleCompletion
import dev.pam.nativeapp.modules.ModuleResultStatus
import dev.pam.nativeapp.modules.NativeModule
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
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class RealtimeModule(@Suppress("UNUSED_PARAMETER") context: android.content.Context) : NativeModule, AutoCloseable {
    private val client=OkHttpClient.Builder().pingInterval(25,TimeUnit.SECONDS).build();private val scheduler=Executors.newSingleThreadScheduledExecutor();private val connections=ConcurrentHashMap<String,Connection>()
    override fun invoke(method:String,payload:ByteArray,completion:ModuleCompletion){runCatching{val values=WireMap.decode(payload);when(method){"connect"->connect(values);"send"->send(values);"poll"->{poll(values,completion);return};"state"->state(values);"close"->close(values);else->error("Unknown method: $method")}}.onSuccess{completion.success(it)}.onFailure{completion.failure(it)}}
    private fun connect(v:Map<String,WireValue>):Map<String,WireValue>{val id=UUID.randomUUID().toString();val connection=Connection(id,v.integer("maxMessageBytes").toInt());connections[id]=connection;val builder=Request.Builder().url(v.text("url"));val headers=JSONObject(v.text("headers"));headers.keys().forEach{key->builder.header(key,headers.getString(key))};v.text("protocols").takeIf{it.isNotEmpty()}?.let{builder.header("Sec-WebSocket-Protocol",it)};connection.socket=client.newWebSocket(builder.build(),connection);return mapOf("identifier" to WireValue.Text(id))}
    private fun send(v:Map<String,WireValue>):Map<String,WireValue>{val c=connection(v);val payload=v.text("payload");val sent=if(v.flag("binary")){val bytes=Base64.decode(payload,Base64.DEFAULT);require(bytes.size<=c.maxMessageBytes){"Message exceeds connection limit"};c.socket?.send(bytes.toByteString())==true}else{require(payload.toByteArray().size<=c.maxMessageBytes){"Message exceeds connection limit"};c.socket?.send(payload)==true};check(sent){"WebSocket rejected message"};return emptyMap()}
    private fun poll(v:Map<String,WireValue>,completion:ModuleCompletion){val c=connection(v);val generation:Int;val ready:Event?;synchronized(c){check(c.waiter==null){"Only one poll may be pending per connection"};ready=c.events.removeFirstOrNull();if(ready==null){c.pollGeneration++;generation=c.pollGeneration;c.waiter=completion}else{generation=c.pollGeneration}};if(ready!=null){completion.success(ready.values());if(ready.kind==4||ready.kind==5)connections.remove(c.id,c);return};val timeout=v.integer("timeoutMillis");scheduler.schedule({val waiter=synchronized(c){if(c.pollGeneration!=generation)null else c.waiter.also{c.waiter=null;c.pollGeneration++}};waiter?.success(Event(7).values())},timeout,TimeUnit.MILLISECONDS)}
    private fun state(v:Map<String,WireValue>)=mapOf("state" to WireValue.Integer(connection(v).state.toLong()))
    private fun close(v:Map<String,WireValue>):Map<String,WireValue>{val c=connection(v);c.state=3;check(c.socket?.close(v.integer("code").toInt(),v.text("reason"))==true){"WebSocket could not close"};return emptyMap()}
    private fun connection(v:Map<String,WireValue>)=connections[v.text("identifier")]?:error("Unknown realtime connection")
    override fun close(){connections.values.forEach{it.socket?.cancel();it.finish(Event(4,code=1001))};connections.clear();scheduler.shutdownNow();client.dispatcher.executorService.shutdown();client.connectionPool.evictAll()}
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
    private data class Event(val kind:Int,val payload:String="",val code:Int=0){fun values()=mapOf("kind" to WireValue.Integer(kind.toLong()),"payload" to WireValue.Text(payload),"code" to WireValue.Integer(code.toLong()))}
    private fun Map<String,WireValue>.text(key:String)=(get(key)as?WireValue.Text)?.value?:error("$key is required")
    private fun Map<String,WireValue>.integer(key:String)=(get(key)as?WireValue.Integer)?.value?:error("$key is required")
    private fun Map<String,WireValue>.flag(key:String)=(get(key)as?WireValue.Flag)?.value?:error("$key is required")
    private fun ModuleCompletion.success(values:Map<String,WireValue>)=complete(ModuleResultStatus.SUCCESS,WireMap.encode(values))
    private fun ModuleCompletion.failure(error:Throwable)=complete(ModuleResultStatus.FAILURE,(error.message?:"Realtime failure").toByteArray())
}
