Summary:
Leadership has expressed an interest in getting a better summary of our usage data in our system. The existing data structures are old, and you’ll need to get them the information while identifying a path forward to make the data clean, reliable and useful.

NOTE: Any technology can be used for each of these steps. 

Fork this repo

In your fork, do the following:

- Create a simple line chart showing Total Usage (MB) per day.
- Answer the following questions:
    - Which `sim_card_id` had the highest total usage?
    - How many usage events resolved to 3G after any cleanup is finished?
    - How many duplicate usage events did you identify?
    - What is the cost of all data used in the linked data?
- Include code, queries, and brief documentation needed to reproduce your work
    - If any frameworks, libraries, or other tools are needed, include them in your documentation.
- Review the provided ERD and describe how you would redesign the database to make the data cleaner, more reliable and useful.
    - What are some risks and tradeoffs with this redesign
    - Include any model considerations, such as keys, constraints, and indices.
    - This can be done as any of the following:
        - A new ERD (if this route, flag constraints/indices somehow)
        - A list of SQL statements building the new models
        - A detailed summary of the changes you would make.
- Document
    - Any Data Quality problems, and how you resolved them
    - Any Questions you might ask about the existing data to clarify your assumptions
    - Assumptions - NOTE: there isn’t necessarily a “one size fits all” answer. We want to see your reasoning.

The new ERD, answers, and chart should all be included in the base folder of the forked repo.
When complete, send a link to your repo to your interviewer.