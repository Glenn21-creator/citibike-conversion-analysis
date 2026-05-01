
# 🚲 From Casual Riders to Loyal Commuters
### A Data-Driven Conversion Strategy for NYC Citibike

![SQL](https://img.shields.io/badge/Tool-BigQuery%20SQL-blue?logo=google-cloud)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen)
![Certificate](https://img.shields.io/badge/Google-Data%20Analytics%20Capstone-4285F4?logo=google)


---

## The Problem

Citibke's finance team knows annual members are more profitable than casual riders. The marketing team's question is simple: **how do we get casual riders to convert?**

Hidden inside the casual Customer population is a cohort of daily commuters who are already riding like Subscribers, however, just paying four to five times more per trip to do it. Finding them, profiling them, and locating them physically in the city is what this project is about.

---

## The Data

This project uses the [`Citi Bike NYC trip dataset`](https://console.cloud.google.com/bigquery/add-data(cameo:product/city-of-new-york/nyc-citi-bike)?project=project-73afdb23-3f97-4a50-abe), a publicly available real-world dataset covering millions of individual bike trips across New York City. The dataset includes trip-level timestamps, station GPS coordinates, user type segmentation (Subscriber / Customer), and critically, birth_year and gender fields that proved central to the key finding of this analysis.
Data privacy: The dataset contains no customer IDs, names, or payment information. No individual rider can be identified or tracked. This constraint directly shaped the recommendations

---

## What Was Built

| Deliverable | Description |
|---|---|
| [`Data Cleaning Pipepeline`](Data-Cleaning-Pipeline.sql) | 10-stage cleaning pipeline — nulls, deduplication, station standardisation, outlier removal |
| [`Exploratory Data Analysis`](Exploratory_Data_Analysis.sql) | Exploratory analysis — duration distributions, seasonal patterns, cohort isolation, station ranking |
| [`The Silent Subscriber Stations`](Top_stations_Hidden_commuters.csv) | Raw query output: 855 stations ranked by Hidden Commuter morning traffic |

---

## Executive Summary

Three numbers tell the whole story:

| | |
|---|---|
| 🔵 **~90%** | Cost saving a casual daily commuter would gain by switching to an annual membership 
| 🟠 **86,08%** | Share of the target cohort who are under 18 — school students riding to class on single-ride tickets on weekdays|
| 🟢 **11,109** | Morning trips through the single highest-traffic station, Central Park S & 6 Ave 

The analysis confirmed an obvious behaviour: Subscribers commute throughout the year, while Customers leisure-ride. But it also revealed something the business didn't know — a hidden group of casual riders who commute every weekday morning, pay leisure prices to do it, and are overwhelmingly young. The station where they ride is known. The message that would resonate with them is obvious. The only missing piece was the data to back it up.


---

## How the Analysis Unfolded

### 1️⃣ Establishing the Behavioural Divide

The first step was building a statistical profile of each user type using percentile analysis (`PERCENTILE_CONT`) and modal duration (`APPROX_TOP_COUNT`).

Subscribers ride short and fast with modal trip duration of **5 minutes**, with **99%** of all rides finishing under **44 minutes**. That 44-minute ceiling is not a coincidence because it sits exactly one minute below Citi Bike's free ride limit, confirming that Subscribers plan their trips deliberately around cost. Customers ride longer and looser with modal trip duration of **12 minutes**, median of **18 minutes**, with rides stretching up to **184 minutes**. Their behaviour is driven by leisure, not efficiency.

![`Citibike Pricing`](Citibike_pricing.png)

The seasonal difference tells an interesting part of the story. At peak summer, Subscribers made **14.9 million trips**. By winter that figure drops to **5.9 million, a 60% decline**, but one that follows a gradual, predictable curve. Customers tell a completely different story. From a summer peak of **2.5 million trips**, casual ridership virtually disappears in winter, collapsing to **300,000, an 88% drop**.

That gap is not just a behavioural difference, it is a revenue risk. Casual rides are dependent on favourable weather, meanwhile annual memberships are not. Hence, every Customer who converts is a guaranteed revenue flow for Citibike that shows up in January regardless of the forecast.

![`Seasonal Trend`](seasonal_trend.png)
---

### 2️⃣ Finding the Hidden Commuters

The core analytical question was: **Are there Customer rides that mimic Subscriber-like riding behaviour?**

A Materialised View `mv_CusRsimSub` was engineered to answer it, filtering the entire Customer population down to rides that match the Subscriber riding behaviour (short morning commutes during weekdays):

```sql
WHERE usertype       = 'Customer'
  AND week_category  = 'Weekday'
  AND time_of_day    = 'Morning'    -- between  7 AM and  10 AM  
  AND tripduration_min BETWEEN 5 AND 20
```

The cohort that emerged (**Hidden Commuters approximately 0,71% of Casual Customer segment**) are casual riders making short, purposeful, weekday morning trips, which may imply that they are not leisure riders who occasionally happen to ride in the morning. They maybe commuters who have never been offered a reason to subscribe.

---

### 3️⃣ The Demographic Surprise

Aggregating the Hidden Commuter cohort against the demographic fields revealed the most significant finding of the project.

**Around 86.1% of Hidden Commuter trips were taken by riders under 18** with gender recorded as Unknown, which may be consistent with younger users skipping optional survey fields. A plausible interpretation will be that these are school students cycling to class every weekday morning using $4.99 single-ride tickets.

![`Age Aggregation`](Age_Distribution.png)

Approximate financial picture for these riders:

| | Single Rides | Annual Membership |
|---|---|---|
| Per trip | $4.99 | ~$1 |
| Per school week (5 days) | ~$50 | ~$5 |
| Per year | ~$1,000+ | $239 |
| **Saving** | — | **~70%** |

---

### 4️⃣ Locating Them on the Map

Since the dataset contains no customer IDs, the docking station is the only place where this audience can be reached. A `FULL OUTER JOIN` on start and end station traffic within the Hidden Commuter segment identified 855 stations with morning activity from this cohort. The top 10 account for 9.7% of all Hidden Commuter interactions.

| Rank | Station | Total Morning Trips |
|---|---|---|
| 🥇 | Central Park S & 6 Ave | 11,109 |
| 🥈 | West St & Chambers St | 7,152 |
| 🥉 | Centre St & Chambers St | 6,969 |
| 4 | Grand Army Plaza & Central Park S | 6,897 |
| 5 | 12 Ave & W 40 St | 6,672 |
| 6 | Pershing Square North | 5,498 |
| 7 | Broadway & W 49 St | 5,157 |
| 8 | E 17 St & Broadway | 5,092 |
| 9 | Washington St & Gansevoort St | 4,759 |
| 10 | Broadway & W 24 St | 4,716 |

The stations cluster geographically around Central Park South, Lower Manhattan's Chambers Street corridor, and Midtown Broadway, the natural transit routes of a student commuter network.

![`NYC map`](Maps.png)

---

## Recommendation

**Go hyper-local, not city-wide.**
Deploy QR-code membership posters exclusively at the Top 10 stations above, starting with Central Park S & 6 Ave, while explicitly portraying the cost advantages associated with opting for an annual membership.

---

## Assumptions & Caveats

- **Station name standardisation** used a frequency-based heuristic — the most common name per station ID was treated as correct. This may not hold where incorrect names were historically dominant.
- **The under-18 finding** is an interpretation based on trip timing and demographic patterns. It is consistent with the data but cannot be verified without additional information.
- **Outlier thresholds** were set at the Customer 99th percentile (184 min) and applied uniformly. Some Subscriber rides in the 44–184 min range may be genuine outliers that were retained.
- **No PII was used at any stage.** All recommendations are station-level precisely because individual targeting is not possible with this data.

---

*Google Data Analytics Professional Certificate — Capstone Case Study 1 | April 2026*
