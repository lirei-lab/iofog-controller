const config = require('../config')
const dbMetrics = require('./db-metrics')

const DEFAULT_MAX_RETRIES = 5
const CONFIG_DEFAULT_MAX_RETRIES = 8
const CONFIG_DEFAULT_BASE_MS = 25

function isSqliteBusyError (error) {
  if (!error) {
    return false
  }
  const messages = [
    error.message,
    error.parent && error.parent.message,
    error.original && error.original.message
  ]
  if (messages.some((message) => message && message.indexOf('SQLITE_BUSY') !== -1)) {
    return true
  }
  // PostgreSQL's equivalents: a deadlock victim (40P01) and a serialization
  // failure (40001) are retried as a whole transaction, which is what Postgres
  // asks of its clients. Seen as a 500 on POST /application/yaml while leaf
  // credentials were refreshed for the same fog.
  const codes = [error.parent && error.parent.code, error.original && error.original.code, error.code]
  return codes.some((code) => code === '40P01' || code === '40001')
}

function sleep (ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function getRetryDefaults () {
  return {
    maxRetries: config.get('settings.dbBusyRetryMaxAttempts', CONFIG_DEFAULT_MAX_RETRIES),
    baseMs: config.get('settings.dbBusyRetryBaseMs', CONFIG_DEFAULT_BASE_MS)
  }
}

/**
 * Retry an async DB operation when SQLite reports SQLITE_BUSY (same semantics as TransactionDecorator queue retries).
 * No-op for non-SQLITE_BUSY errors. Safe on mysql/postgres — busy errors never match.
 */
async function withDbBusyRetry (fn, options = {}) {
  const defaults = getRetryDefaults()
  const maxRetries = options.maxRetries != null ? options.maxRetries : defaults.maxRetries
  const baseMs = options.baseMs != null ? options.baseMs : defaults.baseMs
  let attempt = 0

  while (true) {
    try {
      return await fn()
    } catch (error) {
      if (attempt >= maxRetries || !isSqliteBusyError(error)) {
        throw error
      }
      attempt++
      dbMetrics.recordBusyRetry(options.label)
      const delayMs = baseMs * Math.pow(2, attempt - 1) + Math.floor(Math.random() * baseMs)
      await sleep(delayMs)
    }
  }
}

module.exports = {
  CONFIG_DEFAULT_BASE_MS,
  CONFIG_DEFAULT_MAX_RETRIES,
  DEFAULT_MAX_RETRIES,
  getRetryDefaults,
  isSqliteBusyError,
  withDbBusyRetry
}
