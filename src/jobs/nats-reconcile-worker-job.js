const ClusterControllerService = require('../services/cluster-controller-service')
const NatsService = require('../services/nats-service')
const NatsReconcileTaskManager = require('../data/managers/nats-reconcile-task-manager')
const Config = require('../config')
const logger = require('../logger')
const { runInTransaction, PRIORITY_BACKGROUND } = require('../helpers/transaction-runner')

const scheduleTime = (Config.get('settings.natsReconcileWorkerIntervalSeconds', 3)) * 1000
// Every node's NATS artefacts are re-rendered on this period as well as on events
// an event that did not fire costs at most one period, not for
// ever. Rendering writes a config map only when its content changed, so a pass
// with nothing to correct writes nothing. 0 turns it off.
const periodicTime = (Config.get('settings.natsPeriodicReconcileSeconds', 300)) * 1000
let lastPeriodic = Date.now()
const PERIODIC_KEY = 'natsPeriodicReconcileAt'

// One periodic pass per period across every Controller sharing the database: the
// time of the last one is a Config row, taken by compare-and-swap. Each
// Controller enqueueing its own replaced the other's pending tasks and ran the
// pass twice (measured: 370 tasks in 15 min for 63 nodes, twice the work).
async function claimPeriodicSlot (transaction) {
  const ConfigManager = require('../data/managers/config-manager')
  const Entity = ConfigManager.getEntity()
  const now = Date.now()
  const row = await Entity.findOne({ where: { key: PERIODIC_KEY }, transaction })
  if (!row) {
    try {
      await Entity.create({ key: PERIODIC_KEY, value: String(now) }, { transaction })
      return true
    } catch (error) {
      return false // another Controller created it first
    }
  }
  if (now - Number(row.value) < periodicTime) {
    return false
  }
  const [updated] = await Entity.update({ value: String(now) }, { where: { key: PERIODIC_KEY, value: row.value }, transaction })
  return updated === 1
}

async function enqueuePeriodic () {
  if (!periodicTime || !Config.getBoolean('nats.enabled', false) || Date.now() - lastPeriodic < periodicTime) {
    return
  }
  lastPeriodic = Date.now()
  if (!ClusterControllerService.getCurrentControllerUuid()) {
    return
  }
  const enqueued = await runInTransaction(async (transaction) => {
    if (!(await claimPeriodicSlot(transaction))) {
      return false
    }
    await NatsService.enqueueReconcileTask({ reason: 'periodic' }, transaction)
    return true
  }, { priority: PRIORITY_BACKGROUND, label: 'natsReconcile.enqueuePeriodic' })
  if (enqueued) {
    logger.info('Periodic NATS reconcile enqueued for every node')
  }
}

async function run () {
  try {
    await enqueuePeriodic()
  } catch (error) {
    logger.error({ err: error, msg: 'Periodic NATS reconcile could not be enqueued' })
  }
  try {
    await processNextTask()
  } catch (error) {
    logger.error({ err: error, msg: 'NATS reconcile worker error' })
  } finally {
    setTimeout(run, scheduleTime)
  }
}

async function processNextTask () {
  const uuid = ClusterControllerService.getCurrentControllerUuid()
  if (!uuid) {
    return
  }

  let task
  try {
    task = await NatsService.claimNextTask(uuid)
  } catch (error) {
    logger.error({ err: error, msg: 'NATS reconcile task claim failed' })
    return
  }

  if (!task) {
    return
  }

  const fogUuids = task.fogUuids
    ? task.fogUuids.split(',').map((s) => s.trim()).filter(Boolean)
    : undefined
  const options = {
    reason: task.reason,
    applicationId: task.applicationId,
    accountRuleId: task.accountRuleId,
    userRuleId: task.userRuleId,
    fogUuids: fogUuids && fogUuids.length > 0 ? fogUuids : undefined
  }

  try {
    logger.info(`NATS reconcile task ${task.id} started`)
    await NatsService.reconcileResolverArtifacts(options)
    logger.info(`NATS reconcile task ${task.id} completed`)
    await runInTransaction(async (transaction) => {
      await NatsReconcileTaskManager.getEntity().destroy({
        where: { id: task.id },
        transaction
      })
    }, { priority: PRIORITY_BACKGROUND, label: 'natsReconcile.taskComplete' })
  } catch (error) {
    logger.error({
      err: error,
      msg: `NATS reconcile task ${task.id} failed; task will be reclaimed after staleness`,
      taskId: task.id,
      reason: task.reason
    })
  }
}

module.exports = {
  run
}
