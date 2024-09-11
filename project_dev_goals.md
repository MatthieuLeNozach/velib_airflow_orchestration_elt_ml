

## **Start local...**

The philosophy here is to deploy the most cost efficient, most lightweight web app possible.  
I have capable local machine, but i don't have access to cheap cloud resources...  
As a consequence, whatever needs computing power (ML models) or extensive disk storage (DB aggregations) will be performed locally, then made available in the cloud (S3) 


## **Get familiar with orchestration**

Orchestration is performed via Apache Airflow, precisely the Astro SDK Airflow distribution.

Advantages of Astro SDK:
- The container suite vital to Airflow is abstracted, simple cli commands launch, restart and kill the airflow stack
- The `include` folder is the easiest, most straightforward way to bring external codebase to support the DAGs



## **Get familiar with the ELT process**

Each API call to the Velib service generates 1460+ rows of data, and API calls are performed every 2mn.  
The database has reached a "larger than memory" situation very fast. As a consequence, it is essential to learn how to scale up data storage by using multiple layers of data storage:  
- Ingestion of the API calls: to a **Postgres** database
- Archiving: querying the Postgres DB regularly to store parquet files in local S3 buckets (**MinIO**), for each day of the week, for each week of the year, for each year, etc.
- Transforms: performed on the parquet archives via sql macros with **DuckDB** for its good columnar computing performances and its "larger-than-memory" abilities. Adding DBT to this project would be overkill, but sql transforms are jinja parametrized and stored in sql files to facilitate sql code reusability. Following DBT philosophy, the transform phase is separated into Staging, Intermediary, Mart  



As the project went on between explore and create a pipeline, I could observe the tradeoff between pure SQL and DataFrames for data processing:
- **DataFrames**: 
  - pros: fast data exploration, visualization
  - cons: must avoid "larger-than-memory" situations, slow computations, frequent python API changes making the pipelines unstable 
- **SQL**: 
  - pros: fast computations, fast aggregations, reliable pipelines
  - cons: verbosity of the queries, relative difficulty to mimic chained DataFrame operations

SQL transforms will always offer the most optimized code, and certainly the go-to in a production environment. I acnowledge SQL processing should go as deep as possible in the data pipeline. Past a certain level of data refining however, it doesnt make a sensible difference with DataFrames...




## Explore the data


### The features

One API call returns the current situation in the 1460+ `stationcode`, for each single one of them, we get:, 

**Static features**
- `stationcode`: station number, most of the first numbers are from Paris, last numbers from the peripheral cities
- `name`: The station name, referencing train/subway stations, neighborhoods, streets/street crossings etc
-  `latitude` and `longitude`: gps coordinates
-  `nom_arrondissement_communes`: City name
-  `capacity`: Number of docks at the station


**Variable features**
- `record_timestamp`: a timezone-aware timestamp, received as a string from Velib's API
- `ebikes`: number of electric bikes docked
- `mechanical`: number of mechanical bikes docked
- `numbikesavailable`
- `numdocksavailable` ,


### The KPIs

#### **Global metrics**
**Bikes in circulation (total)**
- **Context**: Velib Metropole is infamous for underdelivering the bike-share service in Paris and suburbs: the urban area covered by it's service is 5 Million +, but there are still a ridiculous amount of bikes available
- Is this amount increasing over the years, though? It would be a nice thing to compare the average of `numbikesavailable` to the past years

**Rush hours**
- **Context**: Velib service may rather be used for work commute than for recreational travels. If such rush hours exist, how to define them<,>
- **Method**: 

**Docks availabiliy**
- **Context**:There is a static number of docks available, and a varying number of bikes. A summed dock availability for all station at each timestamp would give only limited insights
- **Method**: The fluctuation of the summed `numdocksavailable` over the summed total docks may give good service usage trend insights 

**Bikes in circulation (share)**
- **Context**: The sum of bikes in circulation is fluctuating on a day-to-day basis, there can be massive maintenance operations where a large amount of bikes are removed at once, etc.
- **Method**: The daily sum of bikes in circulation must be derived from the amount of `numbikesavailable` when the service is at it's fullest (between 3Am-5Am). For July 2024, it's around 19000 bikes

**Immobilized bikes**
- **Context**: A significant share of bikes in circulation is unfit for usage, hence those bikes staying docked even at rush hour timestamps
- **Method**: The simple global approach to quantifying this problem:
  - Find 1-2 timestamps where `numdocksavailable` is at it's highest (07-08h, 17-18h), and when it's at it's lowest (03-05h)
  - Analyze the amount/share of `ebike`, `mechanical` or `numbikesavailable` that stays docked when the service is stressed





#### **Geographical metrics**
Compared to other major cities like London or Berlin, Paris Metro doesn't sprawl with it's own name. The city of Paris actually stops abruptly past 4-6km from the city center (2 Million inh.). The frontier is represented by a motorway belt, difficult to cross. Although most of Velib service's captive market being located in the outskirts (3 million+ inh.), there is no initiative to push the service availability in the outskirts.   
Let's have a closer look about geographical inequalities:

**Paris VS Banlieue**
- **Context**: The base intuition is that every weekday:
  - Users are migrating from the outskirts of the metropole towards the city center in the morning for their work commute
  - Docking stations are saturated in Paris, but empty in the suburbs for the whole business day
  - Users are migrating back from the city center to the outskirts passed 17h
  - Docking station availability in the outer part of the service becomes problematic past a certain hour (22h) 
- **Method**: compare the the amount/share of `ebike`, `mechanical` or `numbikesavailable` for `nom_arrondissement_communes` being `Paris` or `Banlieue` (everything not Paris) over the day. It would give a good insight about the city center pendular migration


**City/zipcode wise**
- **Context**: Being aware of Paris/Banlieue inequalities is not enough. The data stream only gives Paris as a whole for `nom_arrondissement_communes`, but there are inequalities also within Paris: inner **arrondissements** don't benefit from the same service availability as the outer **arrondissements**
- **Method**: 
  - Use a geo API to retrieve the zip code based on the station's GPS coordinates, update `nom_arrondissement_communes` with zipcode for areas in paris.
  - Aggregate data by zipcode/`nom_arrondissement_communes`

#### **Station-wise metrics**

