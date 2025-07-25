// utils.ts

import { Connection, DescribeSObjectResult } from 'jsforce'
import ora from 'ora'

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

export async function insertWithHierarchyHandling(
    conn: Connection,
    objectName: string,
    records: any[]
): Promise<ResultLog[]> {
    const spinner = ora(`Inserindo registros hierárquicos de ${objectName}...`).start()
    const logs: ResultLog[] = []

    const idMap = new Map<string, string>() // OldId → NewId
    const pending = [...records]
    let insertedCount = 0
    let wave = 0

    while (pending.length > 0) {
        wave++
        const readyToInsert: any[] = []
        const stillPending: any[] = []

        for (const record of pending) {
            const parentId = record.ParentTerritory2Id
            if (!parentId || idMap.has(parentId)) {
                const newParentId = idMap.get(parentId)
                readyToInsert.push({
                    ...record,
                    ParentTerritory2Id: newParentId ?? null,
                })
            } else {
                stillPending.push(record)
            }
        }

        if (readyToInsert.length === 0) {
            spinner.fail(`❌ Não foi possível resolver a hierarquia completa. Registros restantes: ${stillPending.length}`)
            stillPending.forEach((r) => {
                logs.push({
                    Inserido: '❌',
                    IdSalesforce: undefined,
                    Erro: `Parente com ID ${r.ParentTerritory2Id} não encontrado.`,
                })
            })
            break
        }

        const batches = chunkArray(readyToInsert, 200)

        for (const [i, batch] of batches.entries()) {
            const results = await conn.sobject(objectName).create(batch)

            results.forEach((res, j) => {
                const original = batch[j]
                const oldId = original.Id

                if (res.success && oldId) {
                    idMap.set(oldId, res.id!)
                }

                logs.push({
                    Inserido: res.success ? '✅' : '❌',
                    IdSalesforce: res.id,
                    Erro: res.success ? undefined : res.errors?.[0]?.message,
                })
            })

            spinner.info(`🌊 Onda ${wave}, Lote ${i + 1}/${batches.length}: ${results.filter(r => r.success).length}/${batch.length} inseridos`)
            insertedCount += results.filter(r => r.success).length
        }

        pending.length = 0
        pending.push(...stillPending)
    }

    spinner.succeed(`✅ Inserção hierárquica de ${insertedCount} registros de ${objectName} finalizada`)
    return logs
}
