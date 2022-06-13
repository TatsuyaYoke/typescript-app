import * as z from 'zod'

// export const isNotNull = <T>(value: T): value is Exclude<T, null> => value !== null
// export const isNotUndefined = <T>(value: T): value is Exclude<T, undefined> => value !== undefined
export const nonNullable = <T>(value: T): value is NonNullable<T> => value != null
export const isNotNumber = <T>(item: T): item is Exclude<T, number> => typeof item !== 'number'

export type SelectOptionType = {
  label: string
  value: string
}
export type DateSettingType = {
  startDate: Date
  endDate: Date
}
export type RequestTlmType = {
  tlmId: number
  tlmList: string[]
}
export type RequestDataType = {
  project: string
  isOrbit: boolean
  isStored: boolean
  isChosen: boolean
  orbitDatasetPath: string
  groundTestPath: string
  dateSetting: DateSettingType
  testCase: SelectOptionType[]
  tlm: RequestTlmType[]
}

const mode = ['orbit', 'ground'] as const
export type Mode = typeof mode[number]

export type QuerySuccess<T> = { success: true; data: T }
export type QueryError = { success: false; error: string }
export type QueryReturnType<T> = QuerySuccess<T> | QueryError

const regexGroundTestDateTime =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/
// /^[0-9]{4}(-|\/)(0?[1-9]|1[0-2])(-|\/)(0?[1-9]|[12][0-9]|3[01]) ([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/
export const groundDateTimeTypeSchema = z.string().regex(regexGroundTestDateTime)

export const groundDataTypeSchema = z.union([z.number().nullable(), groundDateTimeTypeSchema])
export const groundObjectTypeSchema = z.record(groundDataTypeSchema)

export const groundArrayObjectTypeSchema = z.array(groundObjectTypeSchema)
export const groundObjectArrayTypeSchema = z.record(z.array(groundDataTypeSchema))
export const groundObjectArrayIncludingDateTimeTypeSchema = z
  .object({ DATE: z.array(z.string()) })
  .and(groundObjectArrayTypeSchema)

const regexOrbitDateTime =
  /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9].[0-9]{3}Z/
export const orbitDateTimeTypeSchema = z
  .object({ value: z.string().regex(regexOrbitDateTime) })
  .transform((e) => e.value)

export const orbitDataTypeSchema = z.union([z.number().nullable(), orbitDateTimeTypeSchema])
export const orbitObjectTypeSchema = z.record(orbitDataTypeSchema)

export const orbitArrayObjectTypeSchema = z.array(orbitObjectTypeSchema)
export const orbitObjectArrayTypeSchema = z.record(z.array(orbitDataTypeSchema))
export const orbitObjectArrayIncludingDateTimeTypeSchema = z
  .object({
    OBCTimeUTC: z.array(orbitDateTimeTypeSchema),
    CalibratedOBCTimeUTC: z.array(orbitDateTimeTypeSchema),
  })
  .and(orbitObjectArrayTypeSchema)

export type DateTimeType = {
  orbit: z.infer<typeof orbitDateTimeTypeSchema>
  ground: z.infer<typeof groundDateTimeTypeSchema>
}
export type DataType = {
  orbit: z.infer<typeof orbitDataTypeSchema>
  ground: z.infer<typeof groundDataTypeSchema>
}
export type ArrayObjectType = {
  orbit: z.infer<typeof orbitArrayObjectTypeSchema>
  ground: z.infer<typeof groundArrayObjectTypeSchema>
}
export type ObjectArrayType = {
  orbit: z.infer<typeof orbitObjectArrayTypeSchema>
  ground: z.infer<typeof groundObjectArrayTypeSchema>
}
export type ObjectArrayIncludingDateTimeType = {
  orbit: z.infer<typeof orbitObjectArrayIncludingDateTimeTypeSchema>
  ground: z.infer<typeof groundObjectArrayIncludingDateTimeTypeSchema>
}

export type ResponseDataType<T extends Mode> = {
  success: boolean
  tlm: {
    time: DateTimeType[T][]
    data: {
      [key: string]: DataType[T][]
    }
  }
  errorMessages: string[]
}
