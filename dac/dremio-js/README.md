# Dremio JS SDK

## Usage

```typescript
import { Dremio } from "@dremio/dremio-js";

// Configure the Dremio SDK with your access token and target instance
const dremio = Dremio({
	token: "YOUR_ACCESS_TOKEN",
	origin: "https://your_dremio_instance.example.com:9047",
});

// Create a query
const query = new Query("SELECT * FROM mydata;");

// Run the query
const job = (await dremio.jobs.create(query)).unwrap();

// Subscribe to updates on the job
job.updates.subscribe(() => {
	console.log("Job status updated:", job.details.jobStatus);
});

// List the most recent 20 jobs
for await (let job of dremio.jobs.list({ limit: 20 }).data()) {
	console.log(job);
}
```
