import { prisma } from "../prisma/client";
import redis from "../redis/client";
import { fetchJobs } from "../redis/jobs";

// redis init
// we are pumping our existing game ids into a redis cache
// layer so we can avoid expensive upsert work where
const getGameIds = async () => {
	const getGames = await prisma.game.findMany({
		select: {
			gameId: true,
		},
	});

	const gameIdArray: number[] = [];
	for (const game of getGames) {
		gameIdArray.push(game.gameId);
	}

	gameIdArray.forEach(async (gameId) => {
		await redis.sadd("gameIds", gameId);
	});

	console.log("cache in sync");
};

try {
	getGameIds();
} catch (err) {
	console.log("error syncing db on startup: ", err);
}

// process jobs in the queue
fetchJobs().catch((err) => console.log(`Error fetching jobs: ${err}`));
