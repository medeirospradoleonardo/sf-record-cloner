import 'dotenv/config'
import inquirer from 'inquirer'
import { DescribeSObjectResult } from 'jsforce'
import ora from 'ora'
import { generateExcelReport, RecordResult } from './excel.js'
import { chunkArray, getAllRecords, getExternalIdField, insertCascade, insertWithHierarchyHandling } from './utils.js'
import { loginToOrg } from './auth.js'

const HIERARQUY_OBJECTS = {
  'Territory2': 'ParentTerritory2Id',
  'Pricebook2': 'OriginPricebook__c'
}

export const IGNORE_FIELDS_OBJECTS = {
  'Pricebook2': ['PriceBook__c'],
  'Account': ['TerritoryLkp__c', 'AddressCity__c', 'RequiresApproval__c', 'SegmentacaodoCliente__c', 'Culture__c', 'AccountCredit__c', 'IntegrationResource__c'],
  'OpportunityLineItem': ['TotalPrice'],
  'Opportunity': ['Culture__c', 'PriceListXPaymentCondition__c', 'AccountAddressDelivery__c', 'PriceListSync__c', 'AccountAddressDelivery__c', 'SyncedQuoteId'],
  'PaymentCondition__c': ['OwnerId'],
  'OrderItem': ['OriginalOrderItemId', 'ParentOrderItem__c', 'ContractLineItem__c', 'CustomizedId__c'],
  'Order': ['StatusCode']
}

export const UNIQUE_FIELDS_OBJECTS = {
  'Account': 'SapId__c',
  'ServiceContract': 'SapId__c',
  'Order': 'SapId__c',
  'Quote': 'Name',
  'Opportunity': 'Name',
  'Territory2': 'TerritoryCode__c',
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
    choices: ['Account', 'CultureArea__c', 'AccountShippingRelation__c', 'SalesRule__c', 'DiscountByMarketingSegmentation__c', 'PaymentCategoryCultureRelation_c', 'Pricebook2Assignment__c', 'PaymentCondition__c', 'AccountGrouping__c', 'DiscountByMarketingSegmentation__c', 'AccountContactRelation', 'OpportunityLineItem', 'Order', 'ServiceContract', 'PaymentCondition__c', 'QuoteLineItem', 'ContractLineItem', 'OrderItem', 'UserTerritory2Association']
  }])

  for (const object of objects) {
    const spinner = ora(`Clonando registros de ${object}...`).start()
    try {
      let metadata: DescribeSObjectResult = await connSource.sobject(object).describe()
      const ignoreFields = IGNORE_FIELDS_OBJECTS[object] ?? []
      metadata.fields = metadata.fields.filter((field) => !ignoreFields.includes(field.name))
      let writableFields = metadata.fields.filter(f => f.createable || f.name === 'Id').map(f => f.name)
      let records = (await getAllRecords(connSource, writableFields, object))

      const externalField = await getExternalIdField(connSource, object)

      // Obtém os valores únicos do externalField a partir dos registros que serão inseridos
      const externalFieldsValue = records
        .map((r) => r[externalField])
        .filter((val): val is string => !!val && typeof val === 'string');

      const batchSize = 500; // respeita limites de query do Salesforce
      const externalFieldsValueChunks = chunkArray(externalFieldsValue, batchSize);

      let recordsAlreadyExists: any[] = [];

      for (const chunk of externalFieldsValueChunks) {
        const inClause = chunk.map(v => `'${v.replace(/'/g, "\\'")}'`).join(',');
        const query = `${externalField} IN (${inClause})`;

        const result = await getAllRecords(connDest, writableFields, object, query);

        recordsAlreadyExists.push(...result);
      }

      const recordsAlreadyExistsExternalIds = recordsAlreadyExists.map((record) => record[externalField])

      const recordsToInsertSize = 40000

      const recordsToInsert = records.filter((record) => !recordsAlreadyExistsExternalIds.includes(record[externalField])).slice(0, recordsToInsertSize)
      // console.log(recordsToInsert[0])

      spinner.succeed(`Encontrados ${recordsToInsert.length} registros de ${object} para inserir`)

      let totalSuccess = 0
      const recordsProcessed: RecordResult[] = []

      if (Object.keys(HIERARQUY_OBJECTS).includes(object)) {
        const result = await insertWithHierarchyHandling(connDest, object, HIERARQUY_OBJECTS[object], recordsToInsert)
        totalSuccess = result.filter(r => r.Inserido === '✅').length
        recordsProcessed.push(...result)
      } else {
        const batchSize = 200

        const chunks = chunkArray(recordsToInsert, batchSize)

        let totalResult = []
        let insertedCache = {}

        for (const [index, chunk] of chunks.entries()) {
          const result = await insertCascade(
            connSource,
            connDest,
            object,
            chunk,
            insertedCache
          )

          totalResult.push(...result)

          ora().info(`Chunk ${index + 1}/${chunks.length} finalizada: ${result.filter(r => r.Inserido === '✅').length} processados com sucesso!`)
        }

        totalSuccess = totalResult.filter(r => r.Inserido === '✅').length
        recordsProcessed.push(...totalResult)
      }

      await generateExcelReport(object, recordsToInsert, recordsProcessed)

      ora().succeed(`✅ Total inserido na org destino: ${totalSuccess}`)
    } catch (err: any) {
      spinner.fail(`Erro ao clonar ${object}: ${err.message}`)
    }
  }
}

main()
