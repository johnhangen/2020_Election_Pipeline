SELECT
fips,
county,
state,
(Pop_25_HS/Pop_25_EDUATT)*100 AS per_hs,
((Pop_25_SC + Pop_25_AD + Pop_25_COLL)/Pop_25_EDUATT)*100 AS per_coll,
(Pop_25_GRAD/Pop_25_EDUATT)*100 AS per_grad

FROM edu_att_test;