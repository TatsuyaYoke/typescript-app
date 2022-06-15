const size = ['small', 'medium', 'large'] as const
type Size = typeof size[number]

const FormTypes = {
  personal: 'personal',
  survey: 'survey',
} as const

type FormType = typeof FormTypes[keyof typeof FormTypes]
type EditedType = FormType | null
