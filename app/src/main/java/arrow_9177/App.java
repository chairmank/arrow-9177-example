package arrow_9177;

import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

public class App {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println(
        "Exactly 1 command-line argument is required: "
        + "the destination path to write the Parquet file.");
      System.exit(1);
    }
    Path path = new Path(args[0]);

    Schema avroSchema = Schema.createRecord(
      "example",
      "",
      "",
      false,
      List.of(
        new Schema.Field("a", Schema.create(Schema.Type.STRING))));

    var writer = AvroParquetWriter.builder(path)
      .withSchema(avroSchema)
      .withCompressionCodec(CompressionCodecName.LZ4)
      .withPageSize(1048576)
      .withRowGroupSize(134217728)
      .build();
    try {
      for (int i = 0; i < 10000; i++) {
        GenericRecord record = new GenericRecordBuilder(avroSchema)
          .set("a", UUID.randomUUID().toString())
          .build();
        writer.write(record);
      }
    } finally {
      writer.close();
    }

    System.out.println(String.format("Wrote Parquet file to %s", path));
    HadoopInputFile file = HadoopInputFile.fromPath(path, new Configuration());
    ParquetReadOptions readOptions = ParquetReadOptions.builder().build();
    try (var reader = ParquetFileReader.open(file, readOptions)) {
      ParquetMetadata metadata = reader.getFooter();
      System.out.println(ParquetMetadata.toPrettyJSON(metadata));
    }
  }
}
