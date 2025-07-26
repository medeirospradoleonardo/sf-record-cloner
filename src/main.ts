import 'dotenv/config'
import inquirer from 'inquirer'
import { DescribeSObjectResult } from 'jsforce'
import ora from 'ora'
import { generateExcelReport, RecordResult } from './excel.js'
import { getAllRecords, insertCascade, insertWithHierarchyHandling } from './utils.js'
import { loginToOrg } from './auth.js'

const HIERARQUY_OBJECTS = {
  'Territory2': 'ParentTerritory2Id',
  'Pricebook2': 'OriginPricebook__c'
}

export const IGNORE_FIELDS_OBJECTS = {
  'Pricebook2': ['PriceBook__c'],
  'Account': ['TerritoryLkp__c', 'AddressCity__c', 'RequiresApproval__c', 'SegmentacaodoCliente__c']
}

export const UNIQUE_FIELDS_OBJECTS = {
  'Territory2Reference__c': 'TerritoryCode__c'
}

async function main() {
  const connSource = await loginToOrg(
    process.env.SF_SOURCE_USERNAME!,
    process.env.SF_SOURCE_PASSWORD!,
    'origem'
  )

  const connDest = await loginToOrg(
    process.env.SF_DEST_USERNAME!,
    process.env.SF_DEST_PASSWORD!,
    'destino'
  )

  const allObjects = (await connSource.describeGlobal()).sobjects.map(obj => obj.name)

  const { objects } = await inquirer.prompt<{ objects: string[] }>([{
    type: 'checkbox',
    name: 'objects',
    message: 'Quais objetos você quer clonar?',
    // choices: ['Account', 'Contact', 'Opportunity', 'Lead', 'Territory2', 'City__c', 'Pricebook2', 'Product2', 'Marca__c']
    choices: ['Account']
  }])

  for (const object of objects) {
    const spinner = ora(`Clonando registros de ${object}...`).start()
    try {
      let metadata: DescribeSObjectResult = await connSource.sobject(object).describe()
      const ignoreFields = IGNORE_FIELDS_OBJECTS[object] ?? []
      metadata.fields = metadata.fields.filter((field) => !ignoreFields.includes(field.name))
      let writableFields = metadata.fields.filter(f => f.createable || f.name === 'Id').map(f => f.name)
      const records = (await getAllRecords(connSource, writableFields, object)).slice(0, 4)

      spinner.succeed(`Encontrados ${records.length} registros de ${object}`)

      let totalSuccess = 0
      const recordsProcessed: RecordResult[] = []

      if (Object.keys(HIERARQUY_OBJECTS).includes(object)) {
        const result = await insertWithHierarchyHandling(connDest, object, HIERARQUY_OBJECTS[object], records)
        totalSuccess = result.filter(r => r.Inserido === '✅').length
        recordsProcessed.push(...result)
      } else {
        const result = await insertCascade(
          connSource,
          connDest,
          object,
          records
        )

        totalSuccess = result.filter(r => r.Inserido === '✅').length
        recordsProcessed.push(...result)
      }

      await generateExcelReport(object, records, recordsProcessed)

      ora().succeed(`✅ Total inserido na org destino: ${totalSuccess}`)
    } catch (err: any) {
      spinner.fail(`Erro ao clonar ${object}: ${err.message}`)
    }
  }
}

main()
