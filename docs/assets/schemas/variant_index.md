```
root
 |-- variantId: string (nullable = false)
 |-- chromosome: string (nullable = false)
 |-- position: integer (nullable = false)
 |-- referenceAllele: string (nullable = false)
 |-- alternateAllele: string (nullable = false)
 |-- chromosomeB37: string (nullable = true)
 |-- positionB37: integer (nullable = true)
 |-- alleleType: string (nullable = false)
 |-- alleleFrequencies: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- alleleFrequency: double (nullable = true)
 |    |    |-- populationName: string (nullable = true)
 |-- cadd: struct (nullable = true)
 |    |-- phred: float (nullable = true)
 |    |-- raw: float (nullable = true)
 |-- mostSevereConsequence: string (nullable = true)
 |-- filters: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- rsIds: array (nullable = true)
 |    |-- element: string (containsNull = true)

```
