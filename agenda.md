Workshop Tracks

Cenovus Business Analyst Track [4 hr, 10:45AM - 3:15 PM with 30 minute lunch break - Databricks Leads]

	Who should attend
I am an existing Query user in Databricks
I am someone in the Line of Business. I have deep expertise over my domain, but I might not know how to write code or work with complex technologies. 
Part of my current or future responsibilities is to transform my business data, create reports, and generate insights so that I can bring value to my organization.

Attendee Prerequisites
Have worked with row-column data previously - this could be Excel
Have a basic understanding of SQL (SELECT statements)
Attendees should watch this 10 minute video to set a common baseline Intro to Databricks (Free Edition) | UI Walkthrough

Cenovus IT Setup Prerequisites
Participants need access to “dbw-wus2-prd-cdp-databricks-01”
Participants need access to a catalog and schema and use of a SQL warehouse
Databricks will provide a dataset to be used in the workshop
Participants need to be able to write and run a SQL query

	Workshop Hands-On
Participants will have hands-on labs during each section
Databricks will provide example SQL scripts for participants to reference as they follow along

	At the end of the session the attendees will be able to:
Find data in Unity Catalog and query the data - either by writing SQL or using the Databricks AI Assistant
Build dashboards to visualize the data
Use Genie to ask natural language questions over the data and get answers
User Databricks One
Load data from Unity Catalog into PowerBI and Excel






Agenda

	[60 min, Cenovus Leads] - Databricks Introduction
[10 min] - What does Databricks help you do?
[15 min] - What is Unity Catalog?
[35 min] - How is Databricks Structured at Cenovus
What is CDP?
How do I access Databricks?
What are my permissions?
How do I find data in CDP?

	[60 min, Databricks Leads] - Getting Started in the SQL Editor (
[5 min] - Overview of a table in Unity Catalog
[10 min] - How to query the table data and use the editor
[10 min] - Basic Transformations
[5 min] - What is a view
[30 min] - Hands-On

		BREAK [10 min]
		
	[60 min, Databricks Leads] - Getting Started with Databricks Dashboards
[5 min] - Overview of the Dashboard interface - When to use dashboard vs PowerBI
[10 min] - Adding data to your Dashboard
[10 min] - Adding visuals, tables, text, and filters
[5 min] - Publish, share, refresh the dashboard
genie + dashboard, dashboards are fixed views where genie is dynamic
[30 min] - Hands-On

		BREAK [10 min]

[40 min, Databricks Leads] - How to use an existing Databricks Genie space
[5 min] - Overview of Genie interface & How Genie Works
[5 min] - How to open a Genie Space
[10 min] - How to ask questions to Genie
[20 min] - Hands-On
Show assign certification tab, sample questions, how the SME would curate

		BREAK [10 min]

		[15  min, Databricks & Cenovus Leads] - Databricks One

	[30 min, Databricks & Cenovus Leads] - Integrating with Microsoft Products
[10 min] - Access Databricks data from PowerBI
[10 min] - Access Databricks data from Excel












Cenovus Developer Track [4.5hr, 10:45-3:15 pm Databricks Leads]

	Who should attend
I am an existing Cenovus Developer User on Databricks
I have intermediate or advanced skills in data analysis or data science
I want to learn about running more complex analysis on Databricks

Attendee Prerequisites
Have experience working with notebooks for programming
Have a basic understanding of Python and SQL

Cenovus IT Setup Prerequisites
Participants need access to “dbw-wus2-prd-cdp-databricks-01”
Participants need access to the following compute:
SQL warehouse
Job compute
Interactive Development compute
Participants need USE catalog and USE schema and have the ability to create a table and view
Databricks will provide data and code assets to be added to the workspace ahead of time


	At the end of the session the attendees will be able to:
Create notebooks, version, share, and automate as jobs
Understand how to track costs
Understand how to create tables and views and structure data in a medallion architecture
Use AI SQL/Python functions to turn unstructured and semi-structured data into insights
Build a high-qualify Genie space (text2SQL) that can be embedded in dashboards or used as tools by agents










Agenda
[Databricks Leads] - Welcome (10min) HAYDEN & ANDRIJ SLIDE
Facilitator introductions
Agenda
Cenovus and Databricks Resources for further enablement post-session 

[Databricks Leads] - Databricks Overview (15min) ANDRIJ SLIDE
Unity Catalog Quick Review
Catalog, schema, tables, volumes, registered models and functions
Managed and external
Workspace Navigation
My workspace vs shared space
Creating folders
Feature landscape
Review what tools are available at Cenovus - landscape map
What features are we going to work with today
Share a common workflow
ETL on a schedule, persist as table or create a view, and serve to PowerBI or Databricks Dashboards

[Databricks Leads] - Databricks One (15min) ANDRIJ DEMO
Overview
Cenovus Developers should use Databricks One to share dashboards and genie spaces with their business users
Have the business bookmark the Databricks One page

[Cenovus Leads] - Databricks Cost Management (20min) KURTIS - CENOVUS
Databricks Cost Overview
DBUs and consumption
System tables
Cenovus cost monitoring dashboard 
High-level best practices - interactive vs job vs sql warehouse compute











[Databricks & Cenovus Leads] - Managing Data in Unity Catalog (30min) HAYDEN DEMO
Medallion Architecture - Brett Wiens Cenovus (10min)
Structuring data in a catalog and schema
Few options - align with medallion, align with project, align with dev/test/prd
Tables vs Views
When to use what
Show how in SQL editor
Metadata Management
Importance of metadata for AI and making data discoverable
Adding catalog, schema, and table comments - manual and AI
Adding column comments - manual and AI
Adding tags - align with Cenovus tags
Marking tables as certified

[BREAK] (10min)

[Databricks Leads] - Automating a Notebook in Databricks [45min] ANDRIJ CODE ASSET
Notebook Navigation
Create a folder and/or git folder
Create a notebook
Notebook features
variables, env config, versioning, setting default language, using markdown, choosing cell language
Building a Notebook
Load data
Delta Tables
Volumes
Basic transformations
Create or modify a table
Basic Delta transaction logs
Schedule a Job
Alerts
Triggers
Hands On
Try the challenge activity


[BREAK] (10min)


[Databricks Leads] - Structuring Your Unstructured Data [45min] HAYDEN CODE ASSET
Why Batch Inference Matters
How it benefits analysts, classical ML, and Agentic systems
Brief overview of AI SQL Functions that Databricks offers
Structuring Free Form Text
Classify, Extract, Query
Wrap a custom prompt in a UDF and register to UC
Thoughts on evaluating - create a labelled benchmark, use LLM as a judge
Parsing Documents
Using AI Parse on PDFs
Writing the output to a Delta Table
Hands On (20min)
Try the challenge activity


[Databricks Leads] - Building High Quality Genie Spaces [45min] HAYDEN CODE ASSET
Overview
What is Genie, how does it work, what models are used
Importance of metadata in Unity Catalog (earlier session)
Creating a Genie Space
Adding data
Defining joins, visible columns, add space scoped comments
Defining example SQL queries and instructions
Benchmarking
Monitoring responses
Publish and Share - Databricks One
Hands On (20min)
Build the Genie Space

[Databricks Leads] - Tools in the AI Playground [15min] ANDRIJ DEMO
Playground Overview
Adding Tools
Add our UC Function from earlier
Add our Genie Space

[Databricks Leads] - Closing [5min] HAYDEN & ANDRIJ