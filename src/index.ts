// import sqlite3 from 'sqlite3'
// const query = "select distinct DATE, PCDU_BAT_VOLTAGE, PCDU_BAT_CURRENT from DSX0201_tlm_id_1 where DATE between '2022-04-18' and '2022-04-19'"

import * as z from 'zod'
import { BigQuery } from '@google-cloud/bigquery'
const bigquery = new BigQuery({
  keyFilename: 'G:/共有ドライブ/0705_Sat_Dev_Tlm/settings/strix-tlm-bq-reader-service-account.json',
})

const getStringFromUTCDateFixedTime = (date: Date, time: string) => {
  const year = date.getUTCFullYear().toString()
  const month = ('0' + (date.getUTCMonth() + 1)).slice(-2)
  const day = ('0' + date.getUTCDate()).slice(-2)
  return `${year}-${month}-${day} ${time}`
}

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

const queries = request.tlm.map((element) => {
  const datasetTableQuery = `\n\t\`syns-sol-grdsys-external-prod.${request.bigqueryTable}.tlm_id_${element.tlmId}\``
  const tlmListQuery = element.tlmList.reduce((prev, current) => `${prev}\n\t${current},`, '\n\tOBCTimeUTC,')
  const startDateStr = getStringFromUTCDateFixedTime(request.dateSetting.startDate, '00:00:00')
  const endDateStr = getStringFromUTCDateFixedTime(request.dateSetting.endDate, '23:59:59')
  const dateQuery = `
    \tXR1ReceivedUTC > \'${startDateStr}\'
    \tAND CalibratedOBCTimeUTC > \'2016-1-1 00:00:00 UTC\'
    \tAND OBCTimeUTC > \'${startDateStr}\'
    \tAND OBCTimeUTC < \'${endDateStr}\'
    \tAND OBCTime != 0
    ${request.isStored ? '\tAND Stored = True' : ''}
    `
  const query = `
    SELECT DISTINCT${tlmListQuery}
    FROM${datasetTableQuery}
    WHERE${dateQuery}
    `
  return {
    tlmId: element.tlmId,
    query: query,
  }
})

Promise.all(
  queries.map((element) => {
    return bigquery
      .query(element.query)
      .then((data) => {
        return {
          tlmId: element.tlmId,
          data: data[0],
        }
      })
      .catch(() => {
        return {
          tlmId: element.tlmId,
          data: null,
        }
      })
  })
).then((response) => {
  const data = response[0]?.data
  const mySchema = z.object({ value: z.string() })
  if (data) {
    console.log(data[0].OBCTimeUTC)
    const result = mySchema.safeParse(data[0].OBCTimeUTC)
    console.log(result)
    if (result.success) console.log(result.data)
  }
})
