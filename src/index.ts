import * as z from 'zod'
import { BigQuery } from '@google-cloud/bigquery'

const BIGQUERY_PROJECT = 'syns-sol-grdsys-external-prod'
const OBCTIME_INITIAL = '2016-1-1 00:00:00 UTC'

const getStringFromUTCDateFixedTime = (date: Date, time: string) => {
  const year = date.getUTCFullYear().toString()
  const month = ('0' + (date.getUTCMonth() + 1)).slice(-2)
  const day = ('0' + date.getUTCDate()).slice(-2)
  return `${year}-${month}-${day} ${time}`
}

const queryTrim = (query: string) =>
  query
    .split('\n')
    .map((s) => s.trim())
    .join('\n')
    .replace(/(^\n)|(\n$)/g, '')
    .replace(/^\n/gm, '')
    .replace(/\(tab\)/g, '  ')
    .replace(/,$/, '')

const request = {
  project: 'DSX0201',
  isOrbit: false,
  bigqueryTable: 'strix_b_telemetry_v_6_17',
  isStored: true,
  isChoosed: false,
  dateSetting: {
    startDate: new Date(2022, 3, 28),
    endDate: new Date(2022, 3, 28),
  },
  tesCase: [{ value: '510_FlatSat', label: '510_FlatSat' }],
  tlm: [
    { tlmId: 1, tlmList: ['PCDU_BAT_CURRENT', 'PCDU_BAT_VOLTAGE'] },
    { tlmId: 2, tlmList: ['OBC_AD590_01', 'OBC_AD590_0'] },
  ],
}

const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')

const querSingleTableList = request.tlm.map((currentElement) => {
  const datasetTableQuery = `\n(tab)\`${BIGQUERY_PROJECT}.${request.bigqueryTable}.tlm_id_${currentElement.tlmId}\``
  const tlmListQuery = currentElement.tlmList.reduce(
    (prev, current) => `${prev}\n(tab)${current},`,
    `
    (tab)OBCTimeUTC,
    (tab)CalibratedOBCTimeUTC,
    `
  )
  const whereQuery = `
      (tab)CalibratedOBCTimeUTC > \'${OBCTIME_INITIAL}\'
      (tab)AND OBCTimeUTC BETWEEN \'${startDateStr}\' AND \'${endDateStr}\'
      ${request.isStored ? '(tab)AND Stored = True' : ''}
      `

  const query = queryTrim(`
    SELECT DISTINCT${tlmListQuery}
    FROM${datasetTableQuery}
    WHERE${whereQuery}
    ORDER BY OBCTimeUTC
  `)

  return {
    tlmId: currentElement.tlmId,
    query: query,
  }
})

const bigquery = new BigQuery({
  keyFilename: 'G:/共有ドライブ/0705_Sat_Dev_Tlm/settings/strix-tlm-bq-reader-service-account.json',
})

const bigqueryErrorSchema = z.object({
  reason: z.string(),
  location: z.string(),
  message: z.string(),
})

console.time('test')
Promise.all(
  querSingleTableList.map((element) => {
    return bigquery
      .query(element.query)
      .then((data) => {
        return {
          succuss: true,
          tlmId: element.tlmId,
          data: data[0],
        }
      })
      .catch((err) => {
        const errorParseResult = bigqueryErrorSchema.safeParse(err.errors[0])
        return {
          success: false,
          tlmId: element.tlmId,
          error: errorParseResult.success ? errorParseResult.data : 'Cannot parse error message',
        }
      })
  })
).then((response) => {
  const res = response[1]
  console.log(res)
  console.timeEnd('test')
})
