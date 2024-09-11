

## Main page

- Github link
- Find a title
- Brief description
- Linked table of contents
- Use a map (geopandas?)
- Source links (velib API, INSEE, etc.)
- List of technologies and frameworks used



## Analytics page
- Brief description
- Find most suited field delimiter
- Avoid tabular display of quantitative data at all cost, it must be plotted
- Quantitative analysis / investigation paragraphs must be concise

### Global analysis field
- Internal static facts
  - num of stations,
  - num of docks
  - num of municipalities covered
  - average station density (num of stations per km² and 1 station for r radius)
- External facts
  - amount of inhabitants covered by the service
  - amount of registered users
- Metrics
  - max amount of bikes observed
  - min amount of bikes observed
  - rush hours
- Time-bound metrics
  - total amount of bikes (will captate h/d/w/m seasonalities + global trend)
    - seasonal decomposed plot
  - mean amount of bikes (hour / day)
  - service usage: share of total bikes available over total number of docks
  - service usage trend: total bikes available compared to reference points
    - daily ref point (hour with the highest bike availability ex 3-4am)
    - hourly ref point to get insights about how fast the stations are depleted from bikes

### Global geographic analysis field
- Aggregation by Paris vs Banlieue
  - apply Paris / Banlieue (P/B) zoning to global analysis data... 
  - merge on external facts:
    - number of stations / docks per P/B zone
    - number of inhabitants per P/B zone
    - mean distance between stations
    - (optional) lenth of bike lanes per P/B zone

- Aggregation by municipality (distinct zip codes)
  - apply zip code zoning to global analysis data... 
  - merge on external facts:
    - number of stations / docks per zip code
    - number of inhabitants per zip code
    - mean / median GDP per inhabitant
    - mean distance between stations
    - (optional) lenth of bike lanes per zip code




## Machine Learning page


### Global forecasting field

#### Metrics
- total bikes available
- total ebikes available
- usage volatility (rate of change docks available)

#### Tabular modeling (multivariate regression)
- Baseline (average agg: hour of the day / day of the week)
- XGBoostRegressor

#### Time Series modeling
- Baseline (univariate)
- SARIMAX? (univariate)
- Prophet?
- NN (RNN)? (univariate / multivariate)

#### Additional data
- model: 
  - date of publication
  - size of training set
  - real-time model performance (MAE, RMSE, MAPE, prediction computation time)



### Geographical Zone Forecasting
- #TODO for next milestone