// utils.ts

import { Connection, DescribeSObjectResult } from 'jsforce'
import ora from 'ora'
import { RecordResult } from './excel.js'
import { IGNORE_FIELDS_OBJECTS, UNIQUE_FIELDS_OBJECTS } from './main.js'

export type SObjectRecord = Record<string, any>

export function chunkArray<T>(array: T[], size: number): T[][] {
    return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
        array.slice(i * size, i * size + size)
    )
}

export async function getAllRecords(
    conn: Connection,
    fields: string[],
    objectName: string
): Promise<SObjectRecord[]> {
    const soql = `SELECT ${fields.join(',')} FROM ${objectName}`
    let result = await conn.query<SObjectRecord>(soql)
    let records = result.records

    while (!result.done) {
        result = await conn.queryMore<SObjectRecord>(result.nextRecordsUrl!)
        records = records.concat(result.records)
    }

    return records
}

export async function getRecord(
    conn: Connection,
    fields: string[],
    objectName: string,
    objectId: string
): Promise<SObjectRecord> {
    const soql = `SELECT ${fields.join(',')} FROM ${objectName} WHERE Id = '${objectId}' LIMIT 1`
    let result = await conn.query<SObjectRecord>(soql)
    let records = result.records

    return records?.[0]
}

export async function getWritableFields(
    conn: Connection,
    objectName: string
): Promise<string[]> {
    const metadata: DescribeSObjectResult = await conn.sobject(objectName).describe()
    return metadata.fields
        .filter((f) => f.createable && f.name !== 'Id')
        .map((f) => f.name)
}

export function groupByParentTerritory(records: SObjectRecord[]) {
    const rootTerritories = records.filter(r => !r.ParentTerritoryId)
    const children = records.filter(r => r.ParentTerritoryId)
    return { rootTerritories, children }
}

export async function insertInChunks(
    conn: Connection,
    objectName: string,
    records: SObjectRecord[],
    externalIdMap: Record<string, string> = {}
) {
    const chunks = chunkArray(records, 200)
    const insertedIds: string[] = []
    const results: { success: boolean; id?: string; errors?: any[] }[] = []

    for (const [index, chunk] of chunks.entries()) {
        const resolvedChunk = chunk.map(r => {
            const recordCopy = { ...r }
            for (const field in recordCopy) {
                if (externalIdMap[recordCopy[field]]) {
                    recordCopy[field] = externalIdMap[recordCopy[field]]
                }
            }
            return recordCopy
        })

        const res = await conn.sobject(objectName).create(resolvedChunk)
        insertedIds.push(...res.filter(r => r.success).map(r => r.id!))
        results.push(...res)

        ora().info(`Lote ${index + 1}/${chunks.length} - ${res.filter(r => r.success).length} inseridos`)
    }

    return { insertedIds, results }
}

type ResultLog = {
    Inserido: string
    IdSalesforce?: string
    Erro?: string
}

// Função para identificar os registros pais de um dado registro
function getParentId(record: any, hierarchyField: string) {
    return record[hierarchyField]
}

export async function insertWithHierarchyHandling(
    conn: Connection,
    objectName: string,
    hierarchyField: string,
    records: Record<string, any>[]
): Promise<RecordResult[]> {
    const insertedMap = new Map<string, string>() // Id de origem → Id inserido
    const pending = [...records] // Registros ainda não inseridos
    const insertedResults: RecordResult[] = []

    let iteration = 0
    while (pending.length > 0) {
        const readyToInsert: any[] = []
        const stillPending: any[] = []

        for (const record of pending) {
            const parentId = getParentId(record, hierarchyField)

            // Se não tem pai ou o pai já foi inserido, pode inserir
            if (!parentId || insertedMap.has(parentId)) {
                if (parentId) {
                    record[hierarchyField] = insertedMap.get(parentId)
                }
                readyToInsert.push(record)
            } else {
                stillPending.push(record)
            }
        }

        // Prevenção de loop infinito
        if (readyToInsert.length === 0) {
            throw new Error(
                `Não foi possível resolver a hierarquia em ${objectName}. Verifique se há dependências circulares ou pais faltando.`
            )
        }

        // Insere em lote
        const results = await conn.sobject(objectName).create(readyToInsert)
        for (let i = 0; i < results.length; i++) {
            const originalId = readyToInsert[i].Id
            const result = results[i]

            if (result.success) {
                insertedMap.set(originalId, result.id!)
            }

            insertedResults.push({
                Inserido: result.success ? '✅' : '❌',
                IdSalesforce: result.id,
                Erro: result.success ? undefined : result.errors?.[0]?.message,
            })
        }

        pending.length = 0
        pending.push(...stillPending)
        iteration++
    }

    return insertedResults
}


export type ReferenceField = {
    name: string
    referenceTo: string[]
    relationshipName: string
}

/**
 * Retorna os campos de lookup/referência de um objeto
 */
export function getReferenceFields(metadata: DescribeSObjectResult): ReferenceField[] {
    return metadata.fields
        .filter(
            (f) =>
                Array.isArray(f.referenceTo) && f.referenceTo.length > 0 && !!f.relationshipName
        )
        .map(f => ({
            name: f.name,
            referenceTo: f.referenceTo,
            relationshipName: f.relationshipName!
        } as ReferenceField))
}

/** Busca campo externalId ou unique de um objeto */
export async function getExternalIdField(conn: Connection, objectName: string): Promise<string> {
    const metadata: DescribeSObjectResult = await conn.sobject(objectName).describe()
    const externalField = metadata.fields.find(f => f.externalId)
    if (externalField) return externalField.name
    const uniqueField = metadata.fields.find(f => f.unique && f.name !== 'Id')
    if (uniqueField) return uniqueField.name
    const uniqueFieldName = UNIQUE_FIELDS_OBJECTS[objectName]
    if (uniqueFieldName) return uniqueFieldName
    throw new Error(`Nenhum campo externalId ou unique encontrado para ${objectName}`)
}


const retainOriginalIds = ['User', 'RecordType', 'Group']

/** Inserção em cascata de registros */
export async function insertCascade(
    connSource: Connection,
    connDest: Connection,
    objectName: string,
    records: any[],
    insertedCache: Record<string, Record<string, string>> = {}
): Promise<RecordResult[]> {
    const metadata: DescribeSObjectResult = await connSource.sobject(objectName).describe()
    const ignoreFields = IGNORE_FIELDS_OBJECTS[objectName] ?? []
    metadata.fields = metadata.fields.filter((field) => !ignoreFields.includes(field.name))
    let writableFields = metadata.fields.filter(f => f.createable || f.name === 'Id').map(f => f.name)
    const relationFields = metadata.fields.filter(f => f.referenceTo.length && f.relationshipName && f.createable)

    if (!insertedCache[objectName]) insertedCache[objectName] = {}

    const successResults: RecordResult[] = []
    const toInsert: any[] = []

    for (const record of records) {
        for (const field of relationFields) {
            const relatedId = record[field.name]
            const relatedObject = field.referenceTo[0]

            console.log(relatedObject)
            console.log(record)


            if (!relatedId) continue

            // Se o objeto está na lista de manter ID original
            if (retainOriginalIds.includes(relatedObject)) {
                record[field.name] = relatedId
                continue
            }

            if (!insertedCache[relatedObject]) insertedCache[relatedObject] = {}

            if (!insertedCache[relatedObject][relatedId]) {
                const relatedExternalField = await getExternalIdField(connSource, relatedObject)
                const ignoreFieldsRelatedObject = IGNORE_FIELDS_OBJECTS[relatedObject] ?? []
                let writableFieldsRelatedObject = (await getWritableFields(connSource, relatedObject)).filter((field) => !ignoreFieldsRelatedObject.includes(field))
                const relatedRecord = await getRecord(connSource, writableFieldsRelatedObject, relatedObject, relatedId)
                const externalValue = relatedRecord[relatedExternalField]

                const existing = await connDest
                    .sobject(relatedObject)
                    .findOne({ [relatedExternalField]: externalValue })

                if (!existing) {
                    const relatedResults = await insertCascade(
                        connSource,
                        connDest,
                        relatedObject,
                        [relatedRecord],
                        insertedCache
                    )
                    const idInserted = relatedResults.find(r => r.Inserido === '✅')?.IdSalesforce
                    if (idInserted) {
                        insertedCache[relatedObject][relatedId] = idInserted
                    }
                } else {
                    insertedCache[relatedObject][relatedId] = existing.Id
                }
            }


            if (insertedCache[relatedObject][relatedId]) {
                record[field.name] = insertedCache[relatedObject][relatedId]
            } else {
                throw new Error(`Não foi possível resolver a dependência ${relatedObject} (${relatedId})`)
            }
        }

        toInsert.push(record)
    }

    const result = await connDest.sobject(objectName).create(toInsert, { allOrNone: false })
    console.log(objectName)
    console.log('to Insert' + toInsert)
    console.log(JSON.stringify(result?.[0]?.errors))
    for (let i = 0; i < result.length; i++) {
        const res = result[i]
        const originalId = records[i].Id
        if (res.success) {
            insertedCache[objectName][originalId] = res.id
        }
        successResults.push({
            Inserido: res.success ? '✅' : '❌',
            IdSalesforce: res.id,
            Erro: res.errors?.[0]?.message
        })
    }

    return successResults
}