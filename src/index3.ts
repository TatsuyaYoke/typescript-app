// import sqlite3 from 'sqlite3'
// const query = "select distinct DATE, PCDU_BAT_VOLTAGE, PCDU_BAT_CURRENT from DSX0201_tlm_id_1 where DATE between '2022-04-18' and '2022-04-19'"

// const queryEachTable = queryTrim(
//   request.tlm.reduce((prevQuery, currentElement) => {
//     const datasetTableQuery = `\n(tab)(tab)(tab)\`${BIGQUERY_PROJECT}.${request.bigqueryTable}.tlm_id_${currentElement.tlmId}\``
//     const tlmListQuery = currentElement.tlmList.reduce(
//       (prev, current) => `${prev}\n(tab)(tab)(tab)${current},`,
//       `
//       (tab)(tab)(tab)OBCTimeUTC,
//       (tab)(tab)(tab)CalibratedOBCTimeUTC,
//       `
//     )
//     const whereQuery = `
//       (tab)(tab)(tab)CalibratedOBCTimeUTC > \'${OBCTIME_INITIAL}\'
//       (tab)(tab)(tab)AND OBCTimeUTC BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'
//       ${request.isStored ? '(tab)(tab)(tab)AND Stored = True' : ''}
//       `

//     return `
//       ${prevQuery}
//       (tab)id${currentElement.tlmId} AS (
//         (tab)(tab)SELECT DISTINCT${tlmListQuery}
//         (tab)(tab)FROM${datasetTableQuery}
//         (tab)(tab)WHERE${whereQuery}
//         (tab)(tab)ORDER BY OBCTimeUTC
//         (tab)),
//         `
//   }, '')
// )

// const queryAllCol = queryTrim(
//   request.tlm.reduce((prevQuery, currentElement) => {
//     const tlmListQuery = currentElement.tlmList.reduce((prev, current) => `${prev}\n(tab)${current},`, '')
//     const timeColQuery = `
//     (tab)id${currentElement.tlmId}.OBCTimeUTC AS OBCTimeUTC_id${currentElement.tlmId},
//     (tab)id${currentElement.tlmId}.CalibratedOBCTimeUTC AS CalibratedOBCTimeUTC_id${currentElement.tlmId},
//     `
//     return `${prevQuery}${timeColQuery}${tlmListQuery}`
//   }, 'SELECT DISTINCT')
// )

// const queryJoin = queryTrim(
//   request.tlm.reduce((prevQuery, currentElement, index, array) => {
//     if (index === 0) {
//       return `FROM id${currentElement.tlmId}`
//     }
//     return `
//       ${prevQuery}\n(tab)FULL JOIN id${currentElement.tlmId}
//       (tab)(tab)ON id${array[0]?.tlmId}.OBCTimeUTC = id${currentElement.tlmId}.OBCTimeUTC`
//   }, '')
// )

// const query = `WITH
//     ${queryEachTable}
//     ${queryAllCol}
//     ${queryJoin}
//     WHERE id${request.tlm[0]?.tlmId}.OBCTimeUTC BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'`