import 'dotenv/config'
import inquirer from 'inquirer'
import { DescribeSObjectResult } from 'jsforce'
import ora from 'ora'
import { generateExcelReport, RecordResult } from './excel.js'
import { chunkArray, getAllRecords, insertWithHierarchyHandling } from './utils.js'
import { loginToOrg } from './auth.js'

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
    choices: ['Account', 'Contact', 'Opportunity', 'Lead', 'Territory2']
  }])

  for (const object of objects) {
    const spinner = ora(`Clonando registros de ${object}...`).start()
    try {
      const metadata: DescribeSObjectResult = await connSource.sobject(object).describe()
      const writableFields = metadata.fields.filter(f => f.createable || f.name === 'Id').map(f => f.name)
      const records = await getAllRecords(connSource, writableFields, object)

      spinner.succeed(`Encontrados ${records.length} registros de ${object}`)

      let totalSuccess = 0
      const recordsProcessed: RecordResult[] = []

      if (object === 'Territory2' && writableFields.includes('ParentTerritory2Id')) {
        const result = await insertWithHierarchyHandling(connDest, object, records)
        totalSuccess = result.filter(r => r.Inserido === '✅').length
        recordsProcessed.push(...result)
      } else {
        const batches = chunkArray(records, 200)

        for (const [index, batch] of batches.entries()) {
          const result = await connDest.sobject(object).create(batch)
          const successCount = result.filter(r => r.success).length
          totalSuccess += successCount

          recordsProcessed.push(...result.map((r) => ({
            Inserido: r.success ? '✅' : '❌',
            IdSalesforce: r.id,
            Erro: r.errors?.length ? r.errors[0].message : undefined
          })))

          ora().info(`Lote ${index + 1}/${batches.length} - ${successCount} registros inseridos`)
        }
      }

      await generateExcelReport(object, records, recordsProcessed)

      ora().succeed(`✅ Total inserido na org destino: ${totalSuccess}`)
    } catch (err: any) {
      spinner.fail(`Erro ao clonar ${object}: ${err.message}`)
    }
  }
}

main()
