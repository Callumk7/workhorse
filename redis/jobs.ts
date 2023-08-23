import axios from "axios";
import { Job } from "../types";
import redis from "./client";
import async from "async";

// CONSTANTS
const NUM_WORKERS = 10;
const JOB_QUEUE = "jobs";

const queue = async.queue((job: Job, callback) => {
	processJob(job)
		.then((result) => {
			console.log(`Job ${job.id} processed with result: ${result}`);
			callback();
		})
		.catch(async (err) => {
			console.log(`Error processing job: ${job.id}, error: ${err}`);
			const position = await redis.rpush(JOB_QUEUE, JSON.stringify(job));
			console.log(
				`${job.id}: ${job.type} has been added to the end of the queue, position: ${position}`,
			);
			callback(err);
		});
}, NUM_WORKERS);

queue.error((err, job) => {
	console.log(`Error in job ${job.id}: ${err}`);
});

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

	const res = await axios.post(
		`${process.env.FRONTLINE_URL}/api/${url}`,
		JSON.stringify(job),
	);

	if (res.status === 200) {
		return `Job processed: ${job.id}`;
	} else {
		// I am pretty sure this code is unreachable, as it is thrown to the
		// catch statement in the queue..
		console.log(`Unable to process job ${job.id}, Status: ${res.status}`);
		return `Process Failed, ${job.id} added to queue`;
	}
}

export async function fetchJobs() {
	const BATCH_SIZE = 10;
	const loop = true;
	while (loop) {
		console.log("looping..");
		const rawJobBatch = await redis.lrange(JOB_QUEUE, 0, BATCH_SIZE - 1);
		if (rawJobBatch.length === 0) {
			await new Promise((resolve) => setTimeout(resolve, 8000));
		} else if (rawJobBatch.length > 0) {
			console.log(`jobs popped: ${rawJobBatch.length}`);
			rawJobBatch.forEach((rawJob) => {
				queue.push(JSON.parse(rawJob));
			});
			await redis.ltrim(JOB_QUEUE, rawJobBatch.length, -1);
		}
	}
}
