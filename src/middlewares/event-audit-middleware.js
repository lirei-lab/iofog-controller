const EventService = require('../services/event-service')
const config = require('../config')
const logger = require('../logger')

// An agent's routine traffic -- its status every statusFrequency seconds and its
// change polls -- is telemetry, not an action, and the node's state is already
// kept on its own record. Audited, it was 96 % of the event table (1.06 million
// rows in a week on a 32-node lab fleet, eight indexes each), and those inserts
// filled SQLite's write queue until background work was refused. They are audited only when they fail, or when
// EVENT_AUDIT_AGENT_HEARTBEATS asks for all of them.
const ROUTINE_AGENT_CALLS = [
  ['PUT', '/api/v3/agent/status'],
  ['PATCH', '/api/v3/agent/config/changes'],
  ['PATCH', '/api/v3/agent/config/gps']
]

function isRoutineAgentCall (req) {
  return ROUTINE_AGENT_CALLS.some(([method, path]) => req.method === method && req.path === path)
}

/**
 * Event audit middleware for HTTP requests
 * Tracks all non-GET API operations (POST, PATCH, DELETE, PUT)
 * CRITICAL: This middleware is fully async and non-blocking.
 * Event logging failures will never affect request processing.
 */
function eventAuditMiddleware (req, res, next) {
  // Only track non-GET methods
  if (req.method === 'GET') {
    return next()
  }

  // Don't audit DELETE on events endpoint (to avoid recursion and noise)
  // The DELETE endpoint controller will explicitly create an event AFTER successful deletion
  if (req.method === 'DELETE' && req.path === '/api/v3/events') {
    return next()
  }

  // Check if auditing is enabled (reads from YAML or env var)
  // Use config.get() which properly parses boolean strings from env vars
  const auditEnabled = config.getBoolean('settings.eventAuditEnabled', true)
  if (!auditEnabled) {
    return next()
  }

  // Capture request start time
  const startTime = Date.now()

  // Store original end function
  const originalEnd = res.end

  // Wrap response.end to capture status
  res.end = function (...args) {
    // Call original end first
    originalEnd.apply(this, args)

    if (res.statusCode >= 200 && res.statusCode < 300 && isRoutineAgentCall(req) &&
        !config.getBoolean('settings.eventAuditAgentHeartbeats', false)) {
      return
    }

    // Defer event logging to next tick - NEVER AWAIT
    setImmediate(async () => {
      try {
        // Fire and forget - never await
        EventService.createHttpEvent(req, res, startTime).catch(err => {
          // Silent error handling - never throw
          logger.error({ err }, 'Event logging failed (non-blocking)')
        })
      } catch (error) {
        // Catch any synchronous errors
        logger.error({ err: error }, 'Event logging setup failed (non-blocking)')
        // Don't throw - request already completed
      }
    })
  }

  next()
}

module.exports = eventAuditMiddleware
