# Dremio JS SDK

## Usage

```typescript
import { Dremio } from "@dremio/dremio-js/community";

// Configure the Dremio SDK with your access token and target instance
const dremio = Dremio({
  token: "YOUR_ACCESS_TOKEN",
  origin: "https://your_dremio_instance.example.com:9047",
});

// Create a query
const query = new Query("SELECT * FROM mydata;");

// Run the query
const job = (await dremio.jobs.create(query)).unwrap();

// Subscribe to job status updates
job.updates.subscribe((job) => {
  console.log("Job status updated:", job.status);
});

// Show job results once they're ready
// Each item yielded by this generator is an Apache Arrow `RecordBatch`
for await (let batch of job.results.recordBatches()) {
  console.table([...batch]);
}

// List the most recent 20 jobs
for await (let job of dremio.jobs.list().data().take(20)) {
  console.log(job);
}

// Delete all of the scripts owned by a specific user
for await (let script of dremio.scripts
  .list()
  .data()
  .filter((script) => script.userId === "1234-56-7891")) {
  await script.delete();
}
```
