import * as z from 'zod'

const dataTypeSchema = z.union([z.number().nullable(), z.date()])
const objectTypeSchema = z.record(dataTypeSchema)
const mySchema = z.array(objectTypeSchema)
const objectArrayTypeSchema = z.record(z.array(dataTypeSchema))
const objectArrayTypeIncludeDateSchema = z.object({DATE: z.array(z.date())}).and(objectArrayTypeSchema)

type schemaArrayObjectType = z.infer<typeof mySchema>
type schemaObjectType = z.infer<typeof objectTypeSchema>
type schemaDataType = z.infer<typeof dataTypeSchema>
type ObjectArrayType = z.infer<typeof objectArrayTypeSchema>
type ObjectArrayTypeIncludingDate = z.infer<typeof objectArrayTypeIncludeDateSchema>

// const mySchema = z.array(z.record(z.union([z.number().nullable(), z.date()])))

// type schemaArrayObjectType = z.infer<typeof mySchema>
// type schemaObjectType = schemaArrayObjectType[number]
// type schemaDataType = schemaObjectType[keyof schemaObjectType]
// type ObjectArrayType = {
//   [key: string]: schemaDataType[]
// }
// type ObjectArrayTypeIncludingDate = {
//   DATE: Date[]
//   [key: string]: schemaDataType[]
// }

const includingDate = (
  value: ObjectArrayType | ObjectArrayTypeIncludingDate
): value is ObjectArrayTypeIncludingDate => {
  return (value as ObjectArrayTypeIncludingDate).DATE !== undefined
}

const data = [
  {
    DATE: new Date('2022-05-01 00:00:00 UTC'),
    VOLTAGE: 60,
    CURRENT: 0.1,
  },
  {
    DATE: new Date('2022-05-01 00:00:00 UTC'),
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
