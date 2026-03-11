## Warning
**This is not a recommended approach to convert your rental Actors to PPE**. It will likely result in a penalty to Actor Quality (exact policy will be determined and shared with you in the near future). Use it only as a last resort. There should be a way to make your Actor pricing work well with other approaches, and Apify will help. Try:

- Make your events more expensive than you think; customers are willing to pay for quality
- Charge extra per run, independent of the number of data results
- Introduce multiple events for enhanced data


The code is also a bit old, so don't copy-paste it without any analysis.

This is an example Actor for implementing a rental billing system inside PPE (pay-per-event) billing model. Follow code comments for more details.

The Actor bills a symbolic event price of $0.00001 per event (both `rental` and `result` events) so feel free to test it.
