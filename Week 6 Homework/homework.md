# Homework

Imagine you're in a group of 4 data engineers, you will be in charge of creating the following things:

You are in charge of managing these 5 pipelines that cover the following business areas:
 
- Profit
- Unit-level profit needed for experiments
- Aggregate profit reported to investors
- Growth
- Aggregate growth reported to investors
- Daily growth needed for experiments
- Engagement 
- Aggregate engagement reported to investors

Questions:
1. Who is the primary and secondary owners of these pipelines?
    * Profit
        * Primary owner: Engineer 1
        * Secondary owner: Engineer 2
    * Unit-level profit needed for experiments
        * Primary owner: Engineer 1
        * Secondary owner: Engineer 2
    * Aggregate profit reported to investors
        * Primary owner: Engineer 2
        * Secondary owner: Engineer 1
    * Growth
        * Primary owner: Engineer 2
        * Secondary owner: Engineer 1
    * Aggregate growth reported to investors
        * Primary owner: Engineer 3
        * Secondary owner: Engineer 4
    * Daily growth needed for experiments
        * Primary owner: Engineer 3
        * Secondary owner: Engineer 4
    * Engagement 
        * Primary owner: Engineer 4
        * Secondary owner: Engineer 3
    * Aggregate engagement reported to investors
        * Primary owner: Engineer 4
        * Secondary owner: Engineer 3
2. What is an on-call schedule that is fair? (Think about holidays too!)
    * Each person on the team will be on-call for one week out of the month with another person serving as a backup. Each person will be a backup for one week out of the month. A person will never be on-call two consecutive weeks. Engineers will alternate which holidays they are on-call so the same person isn't on-call for all holidays. When the on-call schedule is planned, the engineers will decide which holidays they would like to take. The engineers cannot be on-call for two consecutive holidays. For example, if an engineer is on-call for Thanksgiving, they will not be on-call for Christmas.
3. Creating run books for all pipelines that report metrics to investors. What could potentially go wrong in these pipelines?
    * The pipelines that report metrics to investors are aggregate profit, aggregate growth, and aggregate engagement. The aggregate engagement pipeline could be skewed causing an out of memory error. The upstream data could have missing data or the schema changed which would prevent the pipeline from running. Backfill would trigger the downstream pipelines.