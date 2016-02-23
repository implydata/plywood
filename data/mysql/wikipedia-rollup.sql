DROP TABLE IF EXISTS `wikipedia`;

CREATE TABLE `wikipedia`
SELECT
  /* Time Spec :-) */
  CONVERT(DATE_FORMAT(`time`, "%Y-%m-%d %H:%i:00"), DATETIME) AS "time", /* Rollup queryGranularity: minute */

  /* Dimensions */
  `sometimeLater`,
  `channel`,
  `cityName`,
  `comment`,
  `commentLength`,
  `countryIsoCode`,
  `countryName`,
  `isAnonymous`,
  `isMinor`,
  `isNew`,
  `isRobot`,
  `isUnpatrolled`,
  `metroCode`,
  `namespace`,
  `page`,
  `regionIsoCode`,
  `regionName`,
  `user`,

  /* Measures */
  COUNT(*) AS "count",
  SUM(`added`) AS "added",
  SUM(`deleted`) AS "deleted",
  SUM(`delta`) AS "delta",
  MIN(`delta`) AS "min_delta",
  MAX(`delta`) AS "max_delta",
  SUM(`deltaByTen`) AS "deltaByTen"

FROM `wikipedia_raw`
GROUP BY
  CONVERT(DATE_FORMAT(`time`, "%Y-%m-%d %H:%i:00"), DATETIME),
  `sometimeLater`,
  `channel`,
  `cityName`,
  `comment`,
  `commentLength`,
  `countryIsoCode`,
  `countryName`,
  `isAnonymous`,
  `isMinor`,
  `isNew`,
  `isRobot`,
  `isUnpatrolled`,
  `metroCode`,
  `namespace`,
  `page`,
  `regionIsoCode`,
  `regionName`,
  `user`;
