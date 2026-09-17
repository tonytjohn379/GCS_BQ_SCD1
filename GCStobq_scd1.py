import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


bigquery_schema = 'ProductID:INTEGER,Name:STRING,Price:INTEGER,Quantity:INTEGER,ManufacturingDate:DATE'


options = PipelineOptions(
    runner='DataflowRunner',
    project='tony1-508206',
    region='us-east1',
    temp_location='gs://product_bucket2/temp',
    staging_location='gs://product_bucket2/staging'
)


def parse_line(line):
    fields = line.split(',')

    return {
        'ProductID': int(fields[0]),
        'Name': fields[1],
        'Price':int(fields[2]),
        'Quantity':int(fields[3]),
        'ManufacturingDate':fields[4]
    }


with beam.Pipeline(options=options) as p:

    (
        p
        | 'Read File from GCS'
        >> beam.io.ReadFromText(
            'gs://product_bucket2/source_file/product_batch2.csv',
            skip_header_lines=1
        )

        | 'Parse CSV'
        >> beam.Map(parse_line)

        | 'Write to Staging'
        >> beam.io.WriteToBigQuery(
            table='tony1-508206:project1.product_staging',
            schema=bigquery_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )
    )