const moment = require('moment')
const Op = require('sequelize').Op

const ChangeTrackingManager = require('../data/managers/change-tracking-manager')

const events = Object.freeze({
  clean: {
    config: false,
    version: false,
    reboot: false,
    deleteNode: false,
    microserviceList: false,
    microserviceConfig: false,
    registries: false,
    tunnel: false,
    prune: false,
    routerChanged: false,
    volumeMounts: false,
    execSessions: false,
    models: false,
    runtimeClasses: false,
    microserviceModels: false,
    knowledge: false,
    microserviceKnowledge: false
  },
  microserviceFull: {
    microserviceConfig: true,
    microserviceList: true,
    microserviceModels: true
  },
  microserviceCommon: {
    microserviceConfig: true,
    microserviceList: true,
    microserviceModels: true
  },
  microserviceList: {
    microserviceList: true
  },
  microserviceConfig: {
    microserviceConfig: true
  },
  version: {
    version: true
  },
  reboot: {
    reboot: true
  },
  deleteNode: {
    deleteNode: true
  },
  registries: {
    registries: true
  },
  tunnel: {
    tunnel: true
  },
  routerChanged: {
    routerChanged: true
  },
  config: {
    config: true
  },
  prune: {
    prune: true
  },
  volumeMounts: {
    volumeMounts: true
  },
  microserviceExecSessions: {
    execSessions: true
  },
  microserviceLogs: {
    microserviceLogs: true
  },
  fogLogs: {
    fogLogs: true
  },
  models: {
    models: true
  },
  runtimeClasses: {
    runtimeClasses: true
  },
  microserviceModels: {
    microserviceModels: true
  },
  knowledge: {
    knowledge: true
  },
  microserviceKnowledge: {
    microserviceKnowledge: true
  }
})

async function create (ioFogUuid, transaction) {
  await ChangeTrackingManager.create({ iofogUuid: ioFogUuid, lastUpdated: moment().toISOString() }, transaction)
}

async function update (ioFogUuid, data, transaction) {
  await ChangeTrackingManager.create({ ...data, iofogUuid: ioFogUuid, lastUpdated: moment().toISOString() }, transaction)
}

async function updateIfChanged (ioFogUuid, data, transaction) {
  await ChangeTrackingManager.create({ ...data, iofogUuid: ioFogUuid, lastUpdated: moment().toISOString() }, transaction)
}

// "<timestamp>|ids:<id>,<id>..." -- what was delivered, carried in lastUpdated
// (a string the agent sends back as it received it). Past MAX_IDS the ids are
// left out and the timestamp cut-off applies, as before.
const DELIVERED_SEP = '|ids:'
const MAX_IDS = 500
function encodeDelivered (lastUpdated, ids) {
  const clean = (ids || []).filter((id) => Number.isInteger(id))
  if (!clean.length || clean.length > MAX_IDS) return lastUpdated
  return `${lastUpdated}${DELIVERED_SEP}${clean.join(',')}`
}

function decodeDelivered (value) {
  const s = String(value || '')
  const at = s.indexOf(DELIVERED_SEP)
  if (at < 0) return { lastUpdated: s, ids: null }
  const ids = s.slice(at + DELIVERED_SEP.length).split(',').map((x) => parseInt(x, 10)).filter((x) => Number.isInteger(x))
  return { lastUpdated: s.slice(0, at), ids }
}

async function resetIfNotUpdated (ioFogUuid, lastUpdated, transaction) {
  const delivered = decodeDelivered(lastUpdated)
  if (delivered.ids && delivered.ids.length) {
    // Exactly the rows handed over.
    await ChangeTrackingManager.delete({ iofogUuid: ioFogUuid, id: { [Op.in]: delivered.ids } }, transaction)
    return
  }
  await ChangeTrackingManager.delete({ iofogUuid: ioFogUuid, lastUpdated: { [Op.lte]: delivered.lastUpdated } }, transaction)
}

async function getByIoFogUuid (ioFogUuid, transaction) {
  return ChangeTrackingManager.findAll({ iofogUuid: ioFogUuid }, transaction)
}

module.exports = {
  events,
  create,
  update,
  updateIfChanged,
  getByIoFogUuid,
  resetIfNotUpdated,
  encodeDelivered,
  decodeDelivered
}
