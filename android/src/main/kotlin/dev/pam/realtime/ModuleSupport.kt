package dev.pam.realtime

import dev.pam.nativeapp.modules.ModuleCompletion
import dev.pam.nativeapp.modules.ModuleResultStatus
import dev.pam.nativeapp.protocol.WireMap
import dev.pam.nativeapp.protocol.WireValue

internal fun Map<String, WireValue>.text(key: String): String =
    (get(key) as? WireValue.Text)?.value ?: throw IllegalArgumentException("$key is required")

internal fun Map<String, WireValue>.integer(key: String): Long =
    (get(key) as? WireValue.Integer)?.value ?: throw IllegalArgumentException("$key is required")

internal fun Map<String, WireValue>.flag(key: String): Boolean =
    (get(key) as? WireValue.Flag)?.value ?: throw IllegalArgumentException("$key is required")

internal fun ModuleCompletion.success(values: Map<String, WireValue> = emptyMap()) =
    complete(ModuleResultStatus.SUCCESS, WireMap.encode(values))

internal fun ModuleCompletion.failure(message: String) =
    complete(ModuleResultStatus.FAILURE, message.toByteArray())
