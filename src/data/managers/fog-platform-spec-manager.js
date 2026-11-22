const BaseManager = require('./base-manager')
const models = require('../models')
const {
  validateFogPlatformSpec,
  parseSpecJson,
  serializeSpecJson,
  pickPlatformSpecFields
} = require('../../schemas/fog-platform-spec')

class FogPlatformSpecManager extends BaseManager {
  getEntity () {
    return models.FogPlatformSpec
  }

  async getParsedSpec (fogUuid, transaction) {
    const row = await this.findOne({ fogUuid }, transaction)
    if (!row) {
      return null
    }
    return {
      fogUuid: row.fogUuid,
      generation: row.generation,
      spec: parseSpecJson(row.specJson)
    }
  }

  async upsertSpec (fogUuid, specObject, transaction) {
    const sanitized = pickPlatformSpecFields(specObject)
    await validateFogPlatformSpec(sanitized)
    const specJson = serializeSpecJson(sanitized)

    const existing = await this.findOne({ fogUuid }, transaction)
    // The same spec is not a new generation: every PATCH of a
    // node -- a tag rewrite with the same tags included -- bumped it, and the
    // caller then reconciled the node's router and NATS and held it Pending.
    if (existing && existing.specJson === specJson) {
      return { fogUuid, generation: existing.generation, specJson, unchanged: true }
    }
    if (existing) {
      const generation = existing.generation + 1
      await this.update({ fogUuid }, { specJson, generation }, transaction)
      return { fogUuid, generation, specJson }
    }

    await this.create({ fogUuid, specJson, generation: 1 }, transaction)
    return { fogUuid, generation: 1, specJson }
  }
}

module.exports = new FogPlatformSpecManager()
