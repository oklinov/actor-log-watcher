import { Actor, ApifyClient, log } from 'apify'
import process from 'node:process'

import { RedashClient, ChartsQueryParameters, ChartsResultItem } from './redashClient.js'

await Actor.init()

type Input = {
    actorId: string
    limit: number
    partOfDate: string
    token: string
}

const input = await Actor.getInput<Input>()
if (!input) throw new Error("Input is missing!");
const { actorId, limit, token, partOfDate } = input

const apifyToken = process.env.APIFY_TOKEN
const client = new ApifyClient({ token: apifyToken })

const queryParams: ChartsQueryParameters = {
    actor_id: actorId,
    limit: limit.toString(),
    part_of_date: partOfDate,
}
log.info(`Query params ${JSON.stringify(queryParams, null, '  ')}`)

const cc = new RedashClient(token, queryParams)
const job = await cc.startQuery()
log.info(`Started charts job: ${job.id}`)

log.info(`Wating for result...`)
const resultId = await cc.waitForResults(job)
// const resultId = 2812356
if (!resultId) {
    throw await Actor.fail('result_id is null')
}
log.info(`Job finished, result id: ${resultId}`)

const results = await cc.getResults(resultId)
log.info(`Got ${results.length} results`)

type RunStats = {
    runId: string
    userId: string
    runLink: string
    userLink: string
    durationSecs: number
    datasetItems: number
    startedAt: string
    finishedAt: string
    status: string
    infos: number
    warns: number
    errors: number
    warnMessages: string[]
    errorMessages: string[]
    buildId: string
    buildVersion: string
    input: any
}

const stats: RunStats[] = []

for (const result of results) {
    const runStats = await getLogsStats(result)
    stats.push(runStats)
}

const sorted = stats.sort((a, b) => (b.warns + b.errors) - (a.warns + a.errors))
for (const runStats of sorted) {
    await Actor.pushData({ ...runStats })
}

async function getLogsStats(res: ChartsResultItem): Promise<RunStats> {
    const {
        dataset_clean_item_count: datasetItems,
        duration_seconds: durationSecs,
        user_id: userId,
        run_id: runId,
        status,
        run_link: runLink,
        user_link: userLink,
        started_at: startedAt,
        finished_at: finishedAt,
        build_id: buildId,
        options_build: buildVersion,
    } = res
    log.info(`Processing run ${runId}`)
    const run = client.run(runId)
    const runLog = await run.log().get() ?? '';
    const input = (await run.keyValueStore().getRecord('INPUT'))?.value
    const lines = runLog.split('\n')
    const stats: RunStats = {
        runId,
        runLink,
        infos: 0,
        warns: 0,
        errors: 0,
        warnMessages: [],
        errorMessages: [],
        datasetItems,
        durationSecs,
        userId,
        userLink,
        status,
        startedAt,
        finishedAt,
        buildId,
        buildVersion,
        input,
    }

    const WARN_LOG = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z WARN/
    const INFO_LOG = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z INFO/
    const ERROR_LOG = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z ERROR/

    for (const line of lines) {
        if (line.match(INFO_LOG)) {
            stats.infos++
        } else if (line.match(WARN_LOG)) {
            stats.warns++
            stats.warnMessages.push(line)
        } else if (line.match(ERROR_LOG)) {
            stats.errors++
            stats.errorMessages.push(line)
        }
    }
    return stats
}

await Actor.exit()
