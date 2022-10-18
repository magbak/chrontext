# chrontext

- __Hybrid Query Engine:__ [SPARQL](https://www.w3.org/TR/sparql11-overview/)- and [Apache Arrow](https://arrow.apache.org/)-based high throughput access to time series data residing in an arbitrary time series database which is contextualized by a knowledge graph. Built in [Rust](https://www.rust-lang.org/) using [pola.rs](https://www.pola.rs/), [spargebra](https://docs.rs/spargebra/latest/spargebra/), [sparesults](https://docs.rs/sparesults/0.1.1/sparesults/) and [oxrdf](https://docs.rs/oxrdf/latest/oxrdf/) from the [Oxigraph](https://github.com/oxigraph/oxigraph) project.  
- __Domain Specific Query Language:__ A customizable query language for accessing time series data using simple generalized paths such as those found in the [Reference Designation System](https://www.iso.org/standard/82229.html) or in [OPC UA](https://opcfoundation.org/about/opc-technologies/opc-ua/) information models. The DSQL is parsed with [nom](https://docs.rs/nom/latest/nom/) and translated to the Hybrid Query language.

Currently, these tools are volatile works in progress, and should not be used by anyone for anything important. 
## Queries
We can make queries in Python. The code assumes that we have a SPARQL-endpoint and an Arrow Flight SQL-endpoint (Dremio) set up. 
```python
import pathlib
from chrontext import Engine, ArrowFlightSQLDatabase, TimeSeriesTable

engine = Engine(OXIGRAPH_QUERY_ENDPOINT)
    tables = [
        TimeSeriesTable(
            schema="my_nas",
            time_series_table="ts.parquet",
            value_column="v",
            timestamp_column="ts",
            identifier_column="id",
            value_datatype="http://www.w3.org/2001/XMLSchema#unsignedInt")
    ]
arrow_flight_sql_database = ArrowFlightSQLDatabase(host=DREMIO_HOST, port=DREMIO_PORT, username="dremio",
                                                   password="dremio123", tables=tables)
engine.set_arrow_flight_sql(arrow_flight_sql_database)
df = engine.execute_hybrid_query("""
PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX chrontext:<https://github.com/magbak/chrontext#>
PREFIX types:<http://example.org/types#>
SELECT ?w ?s ?t ?v WHERE {
    ?w a types:BigWidget .
    ?w types:hasSensor ?s .
    ?s chrontext:hasTimeseries ?ts .
    ?ts chrontext:hasDataPoint ?dp .
    ?dp chrontext:hasTimestamp ?t .
    ?dp chrontext:hasValue ?v .
    FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
}
    """)
```

## Installing pre-built wheels
From the latest [release](https://github.com/magbak/chrontext/releases), copy the appropriate .whl-file for your system, then run:
```shell
pip install https://github.com/magbak/chrontext/releases/download/v0.1.5/chrontext-0.1.12-cp310-cp310-manylinux_2_31_x86_64.whl
```

All code is licensed to [Prediktor AS](https://www.prediktor.com/) under the Apache 2.0 license unless otherwise noted, and has been financed by [The Research Council of Norway](https://www.forskningsradet.no/en/) (grant no. 316656) and [Prediktor AS](https://www.prediktor.com/) as part of a PhD Degree.  