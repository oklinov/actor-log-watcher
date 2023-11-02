import { Actor, ApifyClient, log } from 'apify'
import process from 'node:process'

await Actor.init()

type Input = {
    datasetId: string
}

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

type DatasetItem = {
    user_id: string
    run_id: string
    started_at: string
    finished_at: string
    dataset_clean_item_count: number
    status: string
    duration_seconds: number
    build_id: string
    options_build: string
    run_link: string
    user_link: string
}

const input = await Actor.getInput<Input>()
if (!input) throw new Error("Input is missing!")
const { datasetId } = input

const apifyToken = process.env.APIFY_TOKEN
const client = new ApifyClient({ token: apifyToken })

const datasetClient = client.dataset(datasetId)
const dataset = await datasetClient.get()
if (!dataset) {
    throw Actor.fail(`Could not get dataset ${datasetId}`)
}

const datasetItems = (await datasetClient.listItems()).items as DatasetItem[]

const stats: RunStats[] = []

for (const [index, item] of datasetItems.entries()) {
    log.info(`(${index}/${datasetItems.length}) Processing run ${item.run_id}`)
    const runStats = await getLogsStats(item)
    stats.push(runStats)
}

const sorted = stats.sort((a, b) => (b.warns + b.errors) - (a.warns + a.errors))
for (const runStats of sorted) {
    await Actor.pushData({ ...runStats })
}

async function getLogsStats(item: DatasetItem): Promise<RunStats> {
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
    } = item
    const run = client.run(runId)
    const runLog = await run.log().get() ?? ''
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
