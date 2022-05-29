import * as csv from 'csv'
import * as fs from 'fs'

export const isNotNull = <T>(item: T): item is Exclude<T, null> => item !== null
export const isNotUndefined = <T>(item: T): item is Exclude<T, undefined> => item !== undefined

export type csvDataType = {
  Time: string | number | null
  [key: string]: string | number | null
}

const response: {
  tlm: {
    [key: string]: {
      time: (string | number | null)[]
      data: (string | number | null)[]
    }
  }
  warningMessages: string[]
} = {
  tlm: {
    PCDU_BAT_CURRENT: {
      time: ['2022-04-18 00:00:00', '2022-04-18 00:00:01', '2022-04-18 00:00:02'],
      data: [0, 1, 2],
    },
    PCDU_BAT_VOLTAGE: {
      time: ['2022-04-18 00:00:00', '2022-04-18 00:00:01', '2022-04-18 00:00:02'],
      data: [2, 1, 0],
    },
    OBC_AD590_01: {
      time: ['2022-04-18 00:00:02', '2022-04-18 00:00:03', '2022-04-18 00:00:04'],
      data: [1, 1, 1],
    },
    OBC_AD590_02: {
      time: ['2022-04-18 00:00:02', '2022-04-18 00:00:03', '2022-04-18 00:00:04'],
      data: [2, 2, 2],
    },
  },
  warningMessages: [],
}

const tlmNameList = Object.keys(response.tlm)
const timeList = tlmNameList
  .map((tlmName) => response.tlm[tlmName]?.time)
  .flat()
  .filter(isNotNull)
  .filter(isNotUndefined)
const timeUniqueList = Array.from(new Set(timeList)).sort()

const csvData = timeUniqueList.map((baseTime) => {
  const returnData: csvDataType = { Time: baseTime }
  tlmNameList.forEach((tlmName) => {
    const tlm = response.tlm[tlmName]
    if (tlm) {
      const foundIndex = tlm.time.findIndex((time) => time === baseTime)
      if (foundIndex !== -1) {
        returnData[tlmName] = tlm.data[foundIndex] ?? null
      } else {
        returnData[tlmName] = null
      }
    }
  })
  return returnData
})

csv.stringify(csvData, { header: true }, function (_err, output) {
  fs.writeFileSync('out.csv', output)
})
