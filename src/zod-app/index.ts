import * as z from 'zod'

const dataTypeSchema = z.union([z.number().nullable(), z.string()])
const objectTypeSchema = z.record(dataTypeSchema)
const mySchema = z.array(objectTypeSchema)
const objectArrayTypeSchema = z.record(z.array(dataTypeSchema))
const objectArrayTypeIncludeDateSchema = z.object({ DATE: z.array(z.string()) }).and(objectArrayTypeSchema)
const regex = /^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]/
const dateArraySchema = z.array(z.string().regex(regex))

type schemaArrayObjectType = z.infer<typeof mySchema>
type schemaObjectType = z.infer<typeof objectTypeSchema>
type schemaDataType = z.infer<typeof dataTypeSchema>
type ObjectArrayType = z.infer<typeof objectArrayTypeSchema>
type ObjectArrayTypeIncludingDate = z.infer<typeof objectArrayTypeIncludeDateSchema>
type DateArrayType = z.infer<typeof dateArraySchema>

const includingDate = (
  value: ObjectArrayType | ObjectArrayTypeIncludingDate
): value is ObjectArrayTypeIncludingDate => {
  if ((value as ObjectArrayTypeIncludingDate).DATE !== undefined) {
    const result = dateArraySchema.safeParse(value.DATE)
    if (result.success) {
      result.data.map((value) => {
        console.log(typeof value)
      })
    }
    return result.success
  }
  return false
}

const data = [
  {
    DATE: '2022-04-01 00:00:00 UTC',
    VOLTAGE: 60,
    CURRENT: 0.1,
  },
  {
    DATE: '2022-05-01 00:12:59 UTC',
    VOLTAGE: null,
    CURRENT: 0.1,
  },
]

const result = mySchema.safeParse(data)
if (result.success) {
  const objectArray: ObjectArrayType = {}
  const keys = Object.keys(result.data[0])
  keys.forEach((key) => {
    objectArray[key] = []
  })
  result.data.forEach((record) => {
    keys.forEach((key) => {
      objectArray[key].push(record[key])
    })
  })
  if (includingDate(objectArray)) {
    console.log(objectArray)
  }
} else {
  console.error(result.error.issues)
}
