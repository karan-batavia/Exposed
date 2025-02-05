package org.jetbrains.exposed.sql.vendors

import org.jetbrains.exposed.sql.Index
import org.jetbrains.exposed.sql.Table

class H2DialectMetadata : DatabaseDialectMetadata() {
    override fun existingIndices(vararg tables: Table): Map<Table, List<Index>> = super.existingIndices(*tables)
        .mapValues { entry ->
            entry.value.filterNot { it.indexName.startsWith("PRIMARY_KEY_") }
        }
        .filterValues { it.isNotEmpty() }
}
