import { Job } from "../types";
import redis from "./client";
import async from "async";

// CONSTANTS
const NUM_WORKERS = 20;
const JOB_QUEUE = "jobs";

const queue = async.queue((job: Job, callback) => {
	processJob(job)
		.then((result) => {
			console.log(`Job ${job.id} processed with result: ${result}`);
			callback();
		})
		.catch((err) => {
			console.log(`Error processing job: ${err}`);
			callback(err);
		});
}, NUM_WORKERS);

queue.error((err, job) => {
	console.log(`Error in job ${job.id}: ${err}`);
});

// Jobs are sent to the serverless environment for processing, based on the
// job type. This should be pretty scaleable for now
async function processJob(job: Job) {
	let url: string = "";
	switch (job.type) {
		case "artwork":
			url = "artwork";
			break;
		case "genre":
			url = "genres";
			break;
		case "storyline":
			url = "games";
			break;
		case "rating":
			url = "games";
			break;

		default:
			break;
	}

	const res = await fetch(`${process.env.FRONTLINE_URL}/api/${url}`, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
		},
		body: JSON.stringify(job),
	});

	if (res.ok) {
		console.log(`Job processed: ${job.id}`);
		return "Processed Successfully";
	} else {
		console.log(`Unable to process job ${job.id}, Status: ${res.status}`);
		return "Process Failed";
	}
}

export async function fetchJobs() {
	const BATCH_SIZE = 10;
	const loop = true;
	while (loop) {
		console.log("looping..");
		const rawJobBatch = await redis.lrange(JOB_QUEUE, 0, BATCH_SIZE - 1);
		if (rawJobBatch.length === 0) {
			await new Promise((resolve) => setTimeout(resolve, 5000));
		} else if (rawJobBatch.length > 0) {
			console.log(`jobs popped: ${rawJobBatch.length}`);
			rawJobBatch.forEach((rawJob) => {
				queue.push(JSON.parse(rawJob));
			});
			await redis.ltrim(JOB_QUEUE, rawJobBatch.length, -1);
		}
	}
}

/*
export async function processInitialJobs() {
	let loop = true;
	while (loop) {
		const jobData = await redis.rpop(jobQueue);

		if (jobData) {
			console.log("jobdata from processInitialJobs:", jobData);
			console.log(jobData.charCodeAt(0));
			const job = JSON.parse(jobData);
			await processJob(job);
		} else {
			loop = false;
		}
	}

	listenForJobs();
} 
*/
