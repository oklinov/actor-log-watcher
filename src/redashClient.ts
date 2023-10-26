import axios from 'axios'

export type ChartsJob = {
    id: string
    query_result_id: number | null
}

export type ChartsResultItem = {
    user_id: string,
    run_id: string,
    started_at: string,
    finished_at: string,
    dataset_clean_item_count: number,
    status: string,
    duration_seconds: number,
    build_id: string,
    options_build: string,
    run_link: string,
    user_link: string
}

export type ChartsQueryParameters = {
    actor_id: string
    limit: string
    part_of_date: string
}

export class RedashClient {
    private headers: Record<string, string>

    constructor(token: string, private readonly parameters: ChartsQueryParameters) {
        this.headers = { 'Authorization': `Key ${token}` }
    }

    startQuery = async (): Promise<ChartsJob> => {
        const params = {
            apply_auto_limit: true,
            id: 979,
            max_age: 0,
            parameters: this.parameters,
        }
        const response = await axios.post('https://charts.apify.com/api/queries/979/results', params, {
            headers: this.headers,
        })
        return response.data.job
    }

    fetchJob = async (jobId: string): Promise<ChartsJob> => {
        const response = await axios.get(`https://charts.apify.com/api/jobs/${jobId}`, {
            headers: this.headers,
        })
        return response.data.job
    }

    waitForResults = async (job: ChartsJob): Promise<number | null> => {
        let resultId: number | null = null
        while (resultId === null) {
            const currentJob = await this.fetchJob(job.id)
            if (currentJob.query_result_id) {
                return currentJob.query_result_id
            }
            await new Promise((resolve) => setTimeout(resolve, 4_000))
        }
        return null
    }

    getResults = async (queryResultId: number): Promise<ChartsResultItem[]> => {
        const response = await axios.get(`https://charts.apify.com/api/query_results/${queryResultId}`, {
            headers: this.headers,
        })
        return response.data.query_result.data.rows as ChartsResultItem[]
    }
}
