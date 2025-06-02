import { Actor, log } from 'apify';
import { CheerioCrawler, sleep } from 'crawlee';

// The goal of this PoC is to simulate rental pricing model in the PPE pricing model
// As additional optional feature, we also give 100 free results each mon
// The specification is as follows:
// 1. For every first run in a month, we charge the user a 'rental' event
// 2. First 100 results every month are free, after that all items are charged as 'result' event

// Steps 2 is a special feature. If you don't need it, we recommend skipping it for simplicity

interface FirstMonthRunRecord {
    type: 'first-month-run';
    runId: string;
    timestamp: string;
}

interface ItemsPushedRecord {
    type: 'items-pushed';
    count: number;
    runId: string;
    timestamp: string;
}

type BookkeepingRecord = FirstMonthRunRecord | ItemsPushedRecord;

type ChargeEvent = 'rental' | 'result';

interface Input {
    requestsCount: number;
    prepaidRentalResultsCount?: number;
}

const SAFE_RENTAL_RECHECK_WAIT_MS = 5000;

await Actor.init();

const { requestsCount = 50, prepaidRentalResultsCount = 100 } = (await Actor.getInput<Input>())!;

const yearMonth = new Date().toISOString().slice(0, 7);

// Bookkeping dataset is managed on the creator's account via their token. 
// They are responsible for not deleting active datasets
const bookkeepingClient = Actor.newClient({ token: process.env.BOOKKEEPING_TOKEN || undefined });
const bookkeepingDatasetObject = await bookkeepingClient.datasets().getOrCreate(`${yearMonth}-${Actor.getEnv().userId}`);
const bookkeepingDatasetClient = bookkeepingClient.dataset<BookkeepingRecord>(bookkeepingDatasetObject.id);

const getBookkeepingItems = async (): Promise<BookkeepingRecord[]> => {
    return (await bookkeepingDatasetClient.listItems()).items;
}

const pushBookkeepingData = async (data: BookkeepingRecord | BookkeepingRecord[]) => {
    return await bookkeepingDatasetClient.pushItems(data);
}

let bookkeepingItems = await getBookkeepingItems();

// Check if we are first run to start this month
const wasAlreadyRunThisMonth = bookkeepingItems.some((item) => item.type === 'first-month-run');

if (!wasAlreadyRunThisMonth) {
    // If not, we push to record that we are the first one
    await pushBookkeepingData({
        type: 'first-month-run',
        runId: Actor.getEnv().actorRunId!,
        timestamp: new Date().toISOString(),
    });

    // Now we don't want to charge the high rental charge right away because we could have parallel runs that think they are the firs too
    // So to add assurance, we re-read the dataset again in 5 seconds and only charge if we are the first run that registered in the dataset
    setTimeout(async () => {
        bookkeepingItems = await getBookkeepingItems();

        const isThisReallyFirstRun =
            bookkeepingItems.find((item) => item.type === 'first-month-run')?.runId === Actor.getEnv().actorRunId;
        if (isThisReallyFirstRun) {
            log.info(`Charged for rental event as the first run this month.`);
            await Actor.charge({ eventName: 'rental' as ChargeEvent, count: 1 });
        } else {
            log.info(
                `Wanted to charge for rental but another parallel run already registered as the first run this month so skipping charge.`,
            );
        }
    }, SAFE_RENTAL_RECHECK_WAIT_MS);
} else {
    log.info(`This is not the first run this month, skipping rental charge.`);
}

// Now we took care about the rental part and will start handling the free monthly items
// We have several options how to handle it, depending on how accurate we want to be in parallel runs
// Generally, items will be much cheaper than rental so we can afford to be a bit less accurate here and give users some more free results
// Ordered by simplicity:
// 1. We can just count the bookkeepingItems we already loaded and increment the count by what we push
// 2. We can reload the bookkeepingItems once every 10 seconds or so
// 3. We can reload the bookkeepingItems before we push a new item - This could increase latency and cost for large bookkeeping datasets

// Here we will go with the second option which should be good enough for most use cases

const sumBookkeepingPushedResultsCount = (items: BookkeepingRecord[]) =>
    items.filter((item) => item.type === 'items-pushed').reduce((acc, item) => acc + item.count, 0);

let bookkeepingPushedResultsCount = sumBookkeepingPushedResultsCount(bookkeepingItems);

const getPushedFreeResultsCount = async () => {
    // Reload the bookkeeping items
    // Count how many items we have pushed this month
    return sumBookkeepingPushedResultsCount(await getBookkeepingItems());
};

setInterval(async () => {
    // Every 10 seconds, we check how many items we have pushed this month
    bookkeepingPushedResultsCount = await getPushedFreeResultsCount();
    log.debug(
        `Bookkeeping pushed results count updated: ${bookkeepingPushedResultsCount}/${prepaidRentalResultsCount}`,
    );
}, 10000);

const proxyConfiguration = await Actor.createProxyConfiguration();

const startRequests = Array.from({ length: requestsCount }, (_, i) => ({
    url: `https://example.com/page-${i + 1}`,
}));

// Just a dummy slow crawler to simulate a long-running process
const crawler = new CheerioCrawler({
    proxyConfiguration,
    maxConcurrency: 3,
    requestHandler: async ({ request }) => {
        await sleep(1000); // Simulate some delay

        await Actor.pushData({ url: request.loadedUrl });
        
        // We increment the bookkeping count even before we sync it from the dataset, the sync will override it later
        bookkeepingPushedResultsCount++;

        if (bookkeepingPushedResultsCount < prepaidRentalResultsCount) {
            // If we have not yet pushed enough free results, we push one
            log.info(
                `Pushing free result. ${bookkeepingPushedResultsCount}/${prepaidRentalResultsCount} pushed so far this month.`,
            );
            await pushBookkeepingData({
                type: 'items-pushed',
                count: 1,
                runId: Actor.getEnv().actorRunId!,
                timestamp: new Date().toISOString(),
            });
        } else {
            log.info(
                `Pushed ${bookkeepingPushedResultsCount} results this month, no more free results (up to ${prepaidRentalResultsCount}) available. Charging for this result.`,
            );
            // If we have already pushed enough free results, we charge for this result
            const chargeResult = await Actor.charge({ eventName: 'result' as ChargeEvent, count: 1 });
            if (chargeResult.eventChargeLimitReached) {
                log.warning('Charge limit reached this run. finishing...');
                await crawler.teardown();
            }
        }
    },
});

await crawler.run(startRequests);

await Actor.exit();
