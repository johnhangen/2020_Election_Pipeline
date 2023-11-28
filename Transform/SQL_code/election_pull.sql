SELECT
FIPS,
Code,
County,
Population,
2020W AS 2020_winner,
2020D AS DEM_per,
2020R AS REP_per,
2020O AS OTH_per,
2016W as 2016_winner

FROM elections;