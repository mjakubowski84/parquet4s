// When the user clicks on the search box, we want to toggle the search dropdown
function displayToggleSearch(e) {
  e.preventDefault();
  e.stopPropagation();

  closeDropdownSearch(e);
  
  if (idx === null) {
    console.log("Building search index...");
    prepareIdxAndDocMap();
    console.log("Search index built.");
  }
  const dropdown = document.querySelector("#search-dropdown-content");
  if (dropdown) {
    if (!dropdown.classList.contains("show")) {
      dropdown.classList.add("show");
    }
    document.addEventListener("click", closeDropdownSearch);
    document.addEventListener("keydown", searchOnKeyDown);
    document.addEventListener("keyup", searchOnKeyUp);
  }
}

//We want to prepare the index only after clicking the search bar
var idx = null
const docMap = new Map()

function prepareIdxAndDocMap() {
  const docs = [  
    {
      "title": "Integration with Akka Streams",
      "url": "/parquet4s/docs/akka/",
      "content": "Integration with Akka Streams Parquet4s has an integration module that allows you to read and write Parquet files using Akka Streams. Just import: \"com.github.mjakubowski84\" %% \"parquet4s-akka\" % \"2.19.0\" \"org.apache.hadoop\" % \"hadoop-client\" % yourHadoopVersion ParquetStreams has a single Source for reading a single file or a directory (can be partitioned), a Sinks for writing a single file and a sophisticated Flow for performing complex writes. import akka.NotUsed import akka.actor.ActorSystem import akka.stream.scaladsl.Source import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetStreams, ParquetWriter, Path} import org.apache.parquet.hadoop.ParquetFileWriter.Mode import org.apache.parquet.hadoop.metadata.CompressionCodecName import org.apache.parquet.hadoop.{ParquetWriter =&gt; HadoopParquetWriter} import org.apache.hadoop.conf.Configuration import scala.concurrent.duration._ case class User(userId: String, name: String, created: java.sql.Timestamp) implicit val system: ActorSystem = ActorSystem() val users: Source[User, NotUsed] = ??? val conf: Configuration = ??? // Set Hadoop configuration programmatically // Set Hadoop configuration programmatically // Please check all the available configuration options! val writeOptions = ParquetWriter.Options( writeMode = Mode.OVERWRITE, compressionCodecName = CompressionCodecName.SNAPPY, hadoopConf = conf // optional hadoopConf ) // Writes a single file. users.runWith( ParquetStreams .toParquetSingleFile .of[User] .options(writeOptions) .write(Path(\"file:///data/users/user-303.parquet\")) ) // Tailored for writing indefinite streams. // Writes file when chunk reaches size limit and when defined time period elapses. // Can also partition files! // Check all the parameters and example usage in project sources. users.via( ParquetStreams .viaParquet .of[User] .maxCount(writeOptions.rowGroupSize) .maxDuration(30.seconds) .options(writeOptions) .write(Path(\"file:///data/users\")) ).runForeach(user =&gt; println(s\"Just wrote user ${user.userId}...\")) // Reads a file, files from the directory or a partitioned directory. // Allows parallel reading of Parquet files for speed. // Please also have a look at the rest of parameters. ParquetStreams .fromParquet .as[User] .options(ParquetReader.Options(hadoopConf = conf)) .parallelism(n = 4) .read(Path(\"file:///data/users\")) .runForeach(println) // (Experimental API) Writes a single file using a custom ParquetWriter. class UserParquetWriterBuilder(path: Path) extends HadoopParquetWriter.Builder[User, UserParquetWriterBuilder](path.toHadoop) { override def self() = this override def getWriteSupport(conf: Configuration) = ??? } users.runWith( ParquetStreams .toParquetSingleFile .custom[User, UserParquetWriterBuilder](new UserParquetWriterBuilder(Path(\"file:///data/users/custom.parquet\"))) .options(writeOptions) .write ) Please check examples to learn more."
    } ,    
    {
      "title": "(Experimental) ETL",
      "url": "/parquet4s/docs/experimental/",
      "content": "(Experimental) ETL Version 2.1.0 of Parquet4s introduces advanced operations on generic datasets, that is on ParquetIterable[RowParquetRecord], to the core module. Now users can join and concat two or more datasets which can simplify some ETL jobs a lot. Available operations: Left join Right join Inner join Full join Concat (appending one dataset to another) Write called directly on a dataset. Mind that joins require loading the right-side dataset into memory, so those operations are not applicable for very large datasets. Consider switching the position of datasets in your join operation (the left dataset is iterated over). Or use e.g. Apache Spark which distributes data across multiple machines for performing join operations. Please note that this is an experimental feature. API may change in the future, and some functionalities may be added or removed. import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, Path} import scala.util.Using case class PetOwner(id: Long, name: String, petId: Long, petName: String) // define 1st dataset val readOwners = ParquetReader .projectedGeneric( Col(\"id\").as[Long], Col(\"name\").as[String] ) .read(Path(\"/owners\")) // define 2nd dataset val readPets = ParquetReader .projectedGeneric( Col(\"id\").as[Long].alias(\"petId\"), Col(\"name\").as[String].alias(\"petName\"), Col(\"ownerId\").as[Long] ) .read(Path(\"/pets\")) // join and write output dataset Using.resources(readOwners, readPets) { case (owners, pets) =&gt; owners .innerJoin(right = pets, onLeft = Col(\"id\"), onRight = Col(\"ownerId\")) // define join operation .as[PetOwner] // set typed schema and codecs .writeAndClose(Path(\"/pet_owners/file.parquet\")) // execute all including write to the disk }"
    } ,    
    {
      "title": "Examples",
      "url": "/parquet4s/docs/examples/",
      "content": "Examples Please check examples where you can find simple code covering basics for core, akkaPekko and fs2 modules. Moreover, examples contain two simple applications comprising Akka Streams / Pekko Streams or FS2 and Kafka. They show how you can write partitioned Parquet files with data coming from an indefinite stream."
    } ,    
    {
      "title": "Filtering",
      "url": "/parquet4s/docs/filtering/",
      "content": "Filtering One of the best features of Parquet is an efficient way of filtering. Parquet files contain additional metadata that can be leveraged to drop chunks of data without scanning them. Parquet4s allows users to define filter predicates in order to push filtering out from Scala collections and Akka / Pekko or FS2 stream down to a point before file content is even read. In Akka / Pekko and FS2 filter applies both to the content of files and partitions. You define your filters using simple algebra as follows: import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, Path} case class User(id: Long, email: String, visits: Long) ParquetReader .as[User] .filter(Col(\"email\") === \"user@email.com\" &amp;&amp; Col(\"visits\") &gt;= 100) .read(Path(\"file.parquet\")) You can construct filter predicates using ===, !==, &gt;, &gt;=, &lt;, &lt;=, in and udp operators on columns containing primitive values. You can combine and modify predicates using &amp;&amp;, || and ! operators. in looks for values in a list of keys, similar to SQL’s in operator. Please mind that filtering on java.sql.Timestamp and java.time.LocalDateTime is not supported for Int96 timestamps which is a default type used for timestamps. Consider a different timestamp format for your data to enable filtering. For custom filtering by a column of type T implement UDP[T] trait and use udp operator. import com.github.mjakubowski84.parquet4s.{Col, FilterStatistics, ParquetReader, Path, UDP} case class MyRecord(int: Int) object IntDividesBy10 extends UDP[Int] { private val Ten = 10 // Called for each individual row that belongs to a row group that passed row group filtering. override def keep(value: Int): Boolean = value % Ten == 0 // Called for each row group. // It should contain a logic that eliminates a whole row group if statistics prove that it doesn't contain // data matching the predicate. @inline override def canDrop(statistics: FilterStatistics[Int]): Boolean = { val minMod = statistics.min % Ten val maxMod = statistics.max % Ten (statistics.max - statistics.min &lt; Ten) &amp;&amp; maxMod &gt;= minMod } // Called for each row group for \"not\" predicates. The logic might be different than one in `canDrop`. override def inverseCanDrop(statistics: FilterStatistics[Int]): Boolean = !canDrop(statistics) // called by `toString` override val name: String = \"IntDividesBy10\" } ParquetReader .as[MyRecord] .filter(Col(\"int\").udp(IntDividesBy10)) .read(Path(\"my_ints.parquet\")) Record filter [experimental] RecordFilter is an alternative to filter predicates. It allows to filter records based on record index, that is an ordinal of a record in the file. import com.github.mjakubowski84.parquet4s.{ParquetReader, Path, RecordFilter} case class User(id: Long, email: String, visits: Long) // skips first 10 users ParquetReader .as[User] .filter(RecordFilter(index =&gt; index &gt;= 10)) .read(Path(\"file.parquet\"))"
    } ,    
    {
      "title": "Integration with FS2",
      "url": "/parquet4s/docs/fs2/",
      "content": "Integration with FS2 FS2 integration allows you to read and write Parquet using functional streams. Functionality is exactly the same as in the case of Akka / Pekko module. In order to use it, please import the following: \"com.github.mjakubowski84\" %% \"parquet4s-fs2\" % \"2.19.0\" \"org.apache.hadoop\" % \"hadoop-client\" % yourHadoopVersion parquet object has a single Stream for reading a single file or a directory (can be partitioned), a Pipe for writing a single file and a sophisticated Pipe for performing complex writes. import cats.effect.{IO, IOApp} import com.github.mjakubowski84.parquet4s.parquet.{fromParquet, writeSingleFile, viaParquet} import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path} import fs2.Stream import org.apache.parquet.hadoop.ParquetFileWriter.Mode import org.apache.parquet.hadoop.metadata.CompressionCodecName import org.apache.parquet.hadoop.{ParquetWriter =&gt; HadoopParquetWriter} import org.apache.hadoop.conf.Configuration import scala.concurrent.duration._ object Example extends IOApp.Simple { case class User(userId: String, name: String, created: java.sql.Timestamp) val users: Stream[IO, User] = ??? val conf: Configuration = ??? // Set Hadoop configuration programmatically // Please check all the available configuration options! val writeOptions = ParquetWriter.Options( writeMode = Mode.OVERWRITE, compressionCodecName = CompressionCodecName.SNAPPY, hadoopConf = conf // optional hadoopConf ) // Writes a single file. val writeSingleFilePipe = writeSingleFile[IO] .of[User] .options(writeOptions) .write(Path(\"file:///data/users/single.parquet\")) // (Experimental API) Writes a single file using a custom ParquetWriter. class UserParquetWriterBuilder(path: Path) extends HadoopParquetWriter.Builder[User, UserParquetWriterBuilder](path.toHadoop) { override def self() = this override def getWriteSupport(conf: Configuration) = ??? } val writeSingleFileCustomPipe = writeSingleFile[IO] .custom[User, UserParquetWriterBuilder](new UserParquetWriterBuilder(Path(\"file:///data/users/custom.parquet\"))) .options(writeOptions) .write // Tailored for writing indefinite streams. // Writes file when chunk reaches size limit and when defined time period elapses. // Can also partition files! // Check all the parameters and example usage in project sources. val writeRotatedPipe = viaParquet[IO] .of[User] .maxCount(writeOptions.rowGroupSize) .maxDuration(30.seconds) .options(writeOptions) .write(Path(\"file:///data/users\")) // Reads a file, files from the directory or a partitioned directory. // Allows reading multiple files in parallel for speed. // Please also have a look at the rest of parameters. val readAllStream = fromParquet[IO] .as[User] .options(ParquetReader.Options(hadoopConf = conf)) .parallelism(n = 4) .read(Path(\"file:///data/users\")) .printlns def run: IO[Unit] = users .through(writeRotatedPipe) .through(writeSingleFilePipe) .through(writeSingleFileCustomPipe) .append(readAllStream) .compile .drain } What differentiates FS2 from Akka / Pekko is that, for better performance, FS2 processes stream elements in chunks. Therefore, viaParquet and fromParquet have a chunkSize property that allows a custom definition of the chunk size. The default value is 16. Override the value to set up your own balance between memory consumption and performance. Please check the examples to learn more."
    } ,      
    {
      "title": "Introduction",
      "url": "/parquet4s/docs/",
      "content": "Introduction Parquet4s is a simple I/O for Parquet. Allows you to easily read and write Parquet files in Scala. Use just a Scala case class to define the schema of your data. No need to use Avro, Protobuf, Thrift or other data serialisation systems. You can use generic records if you don’t want to use the case class, too. Compatible with files generated with Apache Spark. However, unlike in Spark, you do not have to start a cluster to perform I/O operations. Based on the official Parquet library, Hadoop Client and Shapeless (Shapeless is not in use in a version for Scala 3). As it is based on Hadoop Client then you can connect to any Hadoop-compatible storage like AWS S3 or Google Cloud Storage. Integrations for Akka Streams, Pekko Streams and FS2. Released for Scala 2.12.x, 2.13.x and 3.3.x."
    } ,      
    {
      "title": "Migration from 1.x",
      "url": "/parquet4s/docs/migration/",
      "content": "Migration from 1.x Records In 1.x ParquetRecord and its implementations were mutable Iterables. In 2.x those classes are immutable. That is, any modification on a record returns a new instance. In 1.x when reading using generic records RowParquetRecord had no entries for missing optional fields. In 2.x each field is represented in RowParquerRecord, the order is kept, and NullValue represents missing data. Type classes SkippingParquetSchemaResolver and SkippingParquetRecordEncoder are removed in 2.x and their logic is merged into regular ParquetSchemaResolver and ParquetRecordEncoder. PartitionLens is removed in 2.x. Its functionality is now available in the API of RowParquetRecord. ValueCodec now composes ValueDecoder and ValueEncoder which allows writing custom encoders or decoders without implementing both. Core API changes Reading Was: ParquetReader .read[Data]( path = \"path/to/file\", options = ParquetReader.Options(), filter = Col(\"id\") &gt; 100 ) Is: ParquetReader .as[Data] .options(ParquetReader.Options()) .filter(Col(\"id\") &gt; 100) .read(Path(\"path/to/file\")) Reading with projection Was: ParquetReader .withProjection[Data] .read( path = \"path/to/file\", options = ParquetReader.Options(), filter = Col(\"id\") &gt; 100 ) Is: ParquetReader .projectedAs[Data] .options(ParquetReader.Options()) .filter(Col(\"id\") &gt; 100) .read(Path(\"path/to/file\")) Reading generic records Was: ParquetReader .read[RowParquetRecord]( path = \"path/to/file\", options = ParquetReader.Options(), filter = Col(\"id\") &gt; 100 ) Is: ParquetReader .generic .options(ParquetReader.Options()) .filter(Col(\"id\") &gt; 100) .read(Path(\"path/to/file\")) Or: ParquetReader .as[RowParquetRecord] .options(ParquetReader.Options()) .filter(Col(\"id\") &gt; 100) .read(Path(\"path/to/file\")) Writing Was: ParquetWriter.writeAndClose( path = \"path/to/file\", data = data, options = ParquetWriter.Options() ) Is: ParquetWriter .of[Data] .options(ParquetWriter.Options()) .writeAndClose(Path(\"path/to/file\"), data) Writing generic records Was: implicit val schema: MessageType = ??? ParquetWriter.writeAndClose( path = \"path/to/file\", data = records, options = ParquetWriter.Options() ) Is: ParquetWriter .generic(schema) .options(ParquetWriter.Options()) .writeAndClose(Path(\"path/to/file\"), records) Or: implicit val schema: MessageType = ??? ParquetWriter .of[RowParquetRecord] .options(ParquetWriter.Options()) .writeAndClose(Path(\"path/to/file\"), records) Stats Was: Stats( path = \"path/to/file\", options = ParquetReader.Options(), filter = Col(\"id\") &gt; 100 ) Is: Stats .builder .options(ParquetReader.Options()) .filter(Col(\"id\") &gt; 100) .stats(Path(\"path/to/file\")) Akka API changes Changes related to generic records are the same as in the core library. Deprecated API of fromParquet, toParquetSequentialWithFileSplit, toParquetParallelUnordered and toParquetIndefinite is removed in 2.x. Reading Was: ParquetStreams .fromParquet[Data] .withOptions(ParquetReader.Options()) .withFilter(Col(\"id\") &gt; 100) .withProjection .read(\"path/to/file\") Is: ParquetStreams .fromParquet .projectedAs[Data] .options(ParquetReader.Options()) .filter(Col(\"id\") &gt; 100) .read(Path(\"path/to/file\")) Note that type class SkippingParquetSchemaResolver that was required for projection is now replaced by regular ParquetSchemaResolver. Writing single file Was: ParquetStreams .toParquetSingleFile( path = \"path/to/file\", options = ParquetWriter.Options() ) Is: ParquetStreams .toParquetSingleFile .of[Data] .options(ParquetWriter.Options()) .write(Path(\"path/to/file\")) Advanced writing Was: ParquetStreams .viaParquet[User](\"path/to/directory\") .withMaxCount(1024 * 1024) .withMaxDuration(30.seconds) .withWriteOptions(ParquetWriter.Options()) .withPartitionBy(\"col1\", \"col2\") .build() Is: ParquetStreams .viaParquet .of[Data] .maxCount(1024 * 1024) .maxDuration(30.seconds) .options(ParquetWriter.Options()) .partitionBy(Col(\"col1\"), Col(\"col2\")) .write(Path(\"path/to/directory\")) In 1.x rotation was executed when maxCount was reached and when maxDuration expired. In 2.x rotation is executed when maxCount is reached or when maxDuration expires. The counter and timer are reset after each rotation. In 1.x all files (all partitions) were rotated at once. In 2.x each file (each partition) is rotated individually. In 1.x preWriteTransformation could produce only a single record. In 2.x preWriteTransformation can produce a collection of records. In 1.x postWriteHandler allowed the implementation of custom rotation of all files. In 2.x postWriteHandler allows the implementation of custom rotation of individual files. Please note the dependency to type class PartitionLens is removed and SkippingParquetSchemaResolver and SkippingParquetRecordEncoder are replaced by regular ParquetSchemaResolver and ParquetRecordEncoder. FS2 API changes Changes related to generic records are the same as in the core library. In 2.x FS2 and Cats Effect are upgraded to version 3.x. Deprecated API of read is removed in 2.x Reading Was: parquet .fromParquet[IO, Data] .options(ParquetReader.Options()) .filter(Col(\"id\") &gt; 100) .projection .read(blocker, \"path/to/file\") Is: parquet .fromParquet[IO] .projectedAs[Data] .options(ParquetReader.Options()) .filter(Col(\"id\") &gt; 100) .read(Path(\"path/to/file\")) Note that type class SkippingParquetSchemaResolver that was required for projection is now replaced by regular ParquetSchemaResolver. Writing single file Was: parquet .writeSingleFile[IO, Data]( blocker = blocker, path = \"path/to/file\", options = ParquetWriter.Options() ) Is: parquet .writeSingleFile[IO] .of[Data] .options(ParquetWriter.Options()) .write(Path(\"path/to/file\")) Advanced writing Was: parquet .viaParquet[IO, User] .maxCount(1024 * 1024) .maxDuration(30.seconds) .options(ParquetWriter.Options()) .partitionBy(\"col1\", \"col2\") .write(blocker, \"path/to/directory\") Is: parquet .viaParquet[IO] .of[Data] .maxCount(1024 * 1024) .maxDuration(30.seconds) .options(ParquetWriter.Options()) .partitionBy(Col(\"col1\"), Col(\"col2\")) .write(Path(\"path/to/directory\")) In 1.x rotation was executed when maxCount was reached and when maxDuration expired. In 2.x rotation is executed when maxCount is reached or when maxDuration expires. The counter and timer are reset after each rotation. In 1.x all files (all partitions) were rotated at once. In 2.x each file (each partition) is rotated individually. In 1.x postWriteHandler allowed the implementation of custom rotation of all files. In 2.x postWriteHandler allows the implementation of custom rotation of individual files. Please note the dependency to type class SkippingParquetSchemaResolver is replaced by regular ParquetSchemaResolver."
    } ,    
    {
      "title": "Partitioning",
      "url": "/parquet4s/docs/partitioning/",
      "content": "Partitioning Parquet4s supports both reading partitions and partitioning data during writing. Writing partitioned data is available only in Akka / Pekko and FS2 modules. Reading partitions is enabled by default in all Parquet4S modules. Akka / Pekko &amp; FS2 Reading partitions is handled by default by fromParquet function. Before data is read Parquet4s scans the directory and resolves partition fields and values. After reading, each record is enriched according to the partition directory tree the file resides in. Writing partitioned data is available in viaParquet. You can specify by which columns data shall be partitioned and Parquet4s will automatically create the proper directory structure, and it will remove the fields from the written records (so that there is no data redundancy). Core Since version 2.12.0 Parquet4S always reads partitioned data. Writing partitions is not supported in core module. Take note! The partition field must be a String. The field cannot belong to the collection. The field cannot be null, be an Option - unless you have defined a default partitioning using viaParquet’s defaultPartition setting. When reading partitioned data make sure that partition directory names follow Hive format. Parquet4s takes care of building proper schemas for partitioned data. However, when you use a custom type and a custom schema definition remember not to include the partition field in the schema — because it is supposed to be encoded as a directory name. In Akka: import akka.NotUsed import akka.actor.ActorSystem import akka.stream.scaladsl.Source import com.github.mjakubowski84.parquet4s.{Col, ParquetStreams, Path} object AkkaExample extends App { implicit val actorSystem: ActorSystem = ActorSystem() import actorSystem.dispatcher case class PartitionDate(year: String, month: String, day: String) case class User(id: Long, name: String, date: PartitionDate) val users: Source[User, NotUsed] = ??? val path = Path(\"path/to/user/directory\") def writePartitionedData = users.via( ParquetStreams .viaParquet .of[User] .partitionBy(Col(\"date.year\"), Col(\"date.month\"), Col(\"date.day\")) .write(path) ).runForeach(user =&gt; println(s\"Just wrote $user\")) def readPartitionedData = ParquetStreams .fromParquet .as[User] .read(path) .runForeach(user =&gt; println(s\"Just read $user\")) for { _ &lt;- writePartitionedData _ &lt;- readPartitionedData _ &lt;- actorSystem.terminate() } yield () } In Pekko: import org.apache.pekko.NotUsed import org.apache.pekko.actor.ActorSystem import org.apache.pekko.stream.scaladsl.Source import com.github.mjakubowski84.parquet4s.{Col, ParquetStreams, Path} object PekkoExample extends App { implicit val actorSystem: ActorSystem = ActorSystem() import actorSystem.dispatcher case class PartitionDate(year: String, month: String, day: String) case class User(id: Long, name: String, date: PartitionDate) val users: Source[User, NotUsed] = ??? val path = Path(\"path/to/user/directory\") def writePartitionedData = users.via( ParquetStreams .viaParquet .of[User] .partitionBy(Col(\"date.year\"), Col(\"date.month\"), Col(\"date.day\")) .write(path) ).runForeach(user =&gt; println(s\"Just wrote $user\"))) def readPartitionedData = ParquetStreams .fromParquet .as[User] .read(path) .runForeach(user =&gt; println(s\"Just read $user\")) for { _ &lt;- writePartitionedData _ &lt;- readPartitionedData _ &lt;- actorSystem.terminate() } yield () } In FS2: import cats.effect.{IO, IOApp} import com.github.mjakubowski84.parquet4s.parquet.{fromParquet, viaParquet} import com.github.mjakubowski84.parquet4s.{Col, Path} import fs2.Stream object FS2Example extends IOApp.Simple { case class PartitionDate(year: String, month: String, day: String) case class User(id: Long, name: String, date: PartitionDate) val users: Stream[IO, User] = ??? val path = Path(\"path/to/user/directory\") val writePipe = viaParquet[IO] .of[User] .partitionBy(Col(\"date.year\"), Col(\"date.month\"), Col(\"date.day\")) .write(path) val readStream = fromParquet[IO] .as[User] .read(path) .printlns def run: IO[Unit] = users .through(writePipe) .append(readStream) .compile .drain } In core: import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, ParquetStreams, Path} object CoreExample extends App { case class PartitionDate(year: String, month: String, day: String) case class User(id: Long, name: String, date: PartitionDate) val path = Path(\"path/to/user/directory\") val users = ParquetReader.as[User].read(path) try users.foreach(println) finally users.close() }"
    } ,    
    {
      "title": "Integration with Pekko Streams",
      "url": "/parquet4s/docs/pekko/",
      "content": "Integration with Pekko Streams Parquet4s has an integration module that allows you to read and write Parquet files using Pekko Streams. Just import: \"com.github.mjakubowski84\" %% \"parquet4s-pekko\" % \"2.19.0\" \"org.apache.hadoop\" % \"hadoop-client\" % yourHadoopVersion ParquetStreams has a single Source for reading a single file or a directory (can be partitioned), a Sinks for writing a single file and a sophisticated Flow for performing complex writes. import org.apache.pekko.NotUsed import org.apache.pekko.actor.ActorSystem import org.apache.pekko.stream.scaladsl.Source import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetStreams, ParquetWriter, Path} import org.apache.parquet.hadoop.ParquetFileWriter.Mode import org.apache.parquet.hadoop.metadata.CompressionCodecName import org.apache.parquet.hadoop.{ParquetWriter =&gt; HadoopParquetWriter} import org.apache.hadoop.conf.Configuration import scala.concurrent.duration._ case class User(userId: String, name: String, created: java.sql.Timestamp) implicit val system: ActorSystem = ActorSystem() val users: Source[User, NotUsed] = ??? val conf: Configuration = ??? // Set Hadoop configuration programmatically // Please check all the available configuration options! val writeOptions = ParquetWriter.Options( writeMode = Mode.OVERWRITE, compressionCodecName = CompressionCodecName.SNAPPY, hadoopConf = conf // optional hadoopConf ) // Writes a single file. users.runWith( ParquetStreams .toParquetSingleFile .of[User] .options(writeOptions) .write(Path(\"file:///data/users/user-303.parquet\")) ) // Tailored for writing indefinite streams. // Writes file when chunk reaches size limit and when defined time period elapses. // Can also partition files! // Check all the parameters and example usage in project sources. users.via( ParquetStreams .viaParquet .of[User] .maxCount(writeOptions.rowGroupSize) .maxDuration(30.seconds) .options(writeOptions) .write(Path(\"file:///data/users\")) ).runForeach(user =&gt; println(s\"Just wrote user ${user.userId}...\")) // Reads a file, files from the directory or a partitioned directory. // Allows reading multiple files in parallel for speed. // Please also have a look at the rest of parameters. ParquetStreams .fromParquet .as[User] .options(ParquetReader.Options(hadoopConf = conf)) .parallelism(n = 4) .read(Path(\"file:///data/users\")) .runForeach(println) // (Experimental API) Writes a single file using a custom ParquetWriter. class UserParquetWriterBuilder(path: Path) extends HadoopParquetWriter.Builder[User, UserParquetWriterBuilder](path.toHadoop) { override def self() = this override def getWriteSupport(conf: Configuration) = ??? } users.runWith( ParquetStreams .toParquetSingleFile .custom[User, UserParquetWriterBuilder](new UserParquetWriterBuilder(Path(\"file:///data/users/custom.parquet\"))) .options(writeOptions) .write ) Please check examples to learn more."
    } ,    
    {
      "title": "Projection",
      "url": "/parquet4s/docs/projection/",
      "content": "Projection Schema projection is a way of optimization of reads. When calling ParquetReader.as[MyData] Parquet4s reads the whole content of each Parquet record even when you provide a case class that maps only a part of stored columns. The same happens when you use generic records by calling ParquetReader.generic. However, you can explicitly tell Parquet4s to use a different schema. In effect, all columns not matching your schema will be skipped and not read. You can define the projection schema in numerous ways: by defining case class for typed read using projectedAs, by defining generic column projection (allows reference to nested fields and aliases) using projectedGeneric, by providing your own instance of Parquet’s MessageType for generic read using projectedGeneric. import com.github.mjakubowski84.parquet4s.{Col, ParquetIterable, ParquetReader, Path, RowParquetRecord} import org.apache.parquet.schema.MessageType // typed read case class MyData(column1: Int, columnX: String) val myData: ParquetIterable[MyData] = ParquetReader .projectedAs[MyData] .read(Path(\"file.parquet\")) // generic read with column projection val records1: ParquetIterable[RowParquetRecord] = ParquetReader .projectedGeneric( Col(\"column1\").as[Int], Col(\"columnX\").as[String].alias(\"my_column\"), ) .read(Path(\"file.parquet\")) // generic read with own instance of Parquet schema val schemaOverride: MessageType = ??? val records2: ParquetIterable[RowParquetRecord] = ParquetReader .projectedGeneric(schemaOverride) .read(Path(\"file.parquet\"))"
    } ,    
    {
      "title": "Read and write Parquet from and to Protobuf",
      "url": "/parquet4s/docs/protobuf/",
      "content": "Read and write Parquet from and to Protobuf Using the original Java Parquet library, you can read and write parquet to and from Protbuf. Parquet4s has custom functions in its API, which could be leveraged for that. However, Parquet Protobuf can only be used with Java models, not to mention other issues that make it hard to use, especially in Scala. You would prefer to use ScalaPB in Scala projects, right? Thanks to Parquet4S, you can! Import ScalaPB extension to any Parquet4S project, either it is Akka / Pekko, FS2 or plain Scala: \"com.github.mjakubowski84\" %% \"parquet4s-scalapb\" % \"2.19.0\" Follow the ScalaPB documentation to generate your Scala model from .proto files. Then, import Parquet4S type classes tailored for Protobuf. The rest of the code stays the same as in regular Parquet4S - no matter if that is Akka / Pekko, FS2 or core! import com.github.mjakubowski84.parquet4s.ScalaPBImplicits._ import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path} import scala.util.Using case class GeneratedProtobufData(someField: Int) val data: Iterable[GeneratedProtobufData] = ??? // your data // your data val path: Path = ??? // path to write to / to read from // path to write to / to read from // write ParquetWriter.of[GeneratedProtobufData].writeAndClose(path.append(\"data.parquet\"), data) // read Using.resource(ParquetReader.as[GeneratedProtobufData].read(path))(_.foreach(println)) Please follow the examples to learn more."
    } ,    
    {
      "title": "Quick start",
      "url": "/parquet4s/docs/quick_start/",
      "content": "Quick start SBT libraryDependencies ++= Seq( \"com.github.mjakubowski84\" %% \"parquet4s-core\" % \"2.19.0\", \"org.apache.hadoop\" % \"hadoop-client\" % yourHadoopVersion ) Mill def ivyDeps = Agg( ivy\"com.github.mjakubowski84::parquet4s-core:2.19.0\", ivy\"org.apache.hadoop:hadoop-client:$yourHadoopVersion\" ) import com.github.mjakubowski84.parquet4s.{ ParquetReader, ParquetWriter, Path } case class User(userId: String, name: String, created: java.sql.Timestamp) val users: Iterable[User] = Seq( User(\"1\", \"parquet\", new java.sql.Timestamp(1L)) ) val path = Path(\"path/to/local/file.parquet\") // writing ParquetWriter.of[User].writeAndClose(path, users) // reading val parquetIterable = ParquetReader.as[User].read(path) try { parquetIterable.foreach(println) } finally parquetIterable.close() AWS S3 In order to connect to AWS S3 you need to define one more dependency: \"org.apache.hadoop\" % \"hadoop-aws\" % yourHadoopVersion Next, the most common way is to define following environmental variables: export AWS_ACCESS_KEY_ID=my.aws.key export AWS_SECRET_ACCESS_KEY=my.secret.key You may need to set some configuration properties to access your storage, e.g. fs.s3a.path.style.access. Please follow documentation of Hadoop AWS for more details and troubleshooting. Passing Hadoop Configs Programmatically File system configs for S3, GCS, Hadoop, etc. can also be set programmatically to the ParquetReader and ParquetWriter by passing the Configuration to the ParqetReader.Options and ParquetWriter.Options case classes. import com.github.mjakubowski84.parquet4s.{ ParquetReader, ParquetWriter, Path } import org.apache.parquet.hadoop.metadata.CompressionCodecName import org.apache.hadoop.conf.Configuration case class User(userId: String, name: String, created: java.sql.Timestamp) val users: Iterable[User] = Seq( User(\"1\", \"parquet\", new java.sql.Timestamp(1L)) ) val hadoopConf = new Configuration() hadoopConf.set(\"fs.s3a.path.style.access\", \"true\") val writerOptions = ParquetWriter.Options( compressionCodecName = CompressionCodecName.SNAPPY, hadoopConf = hadoopConf ) ParquetWriter .of[User] .options(writerOptions) .writeAndClose(Path(\"path/to/local/file.parquet\"), users)"
    } ,    
    {
      "title": "Records, types and schema",
      "url": "/parquet4s/docs/records_and_schema/",
      "content": "Records A data entry in Parquet is called a record. The record can represent a row of data, or it can be a nested complex field in another row. Other types of record are a map and a list. Stored data must be organised in rows. Neither primitive type nor map or list are allowed as a root data type. In Parquet4s those concepts are represented by types that extend ParquetRecord: RowParquetRecord, MapParquetRecord and ListParquetRecord. ParquetRecord extends Scala’s immutable Iterable and allows iteration (next to many other operations) over its content: fields of a row, key-value entries of a map and elements of a list. When using the library you have the option to use those data structures directly, or you can use regular Scala classes that are encoded/decoded by instances of ValueCodec to/from ParqutRecord. Parquet organizes row records into pages and pages into row groups. Row groups and pages are data blocks accompanied by metadata such as statistics and dictionaries. Metadata is leveraged during filtering - so that some data blocks are skipped during reading if the metadata proves that the related data does not match provided filter predicate. For more information about data structures in Parquet please refer to the official documentation. Schema Each Parquet file contains the schema of the data it stores. The schema defines the structure of records, names and types of fields, optionality, etc. Schema is required for writing Parquet files and can be optionally used during reading for projection. The official Parquet library, that Parquet4s is based on, defines the schema in Java type called MessageType. As it is quite tedious to define the schema and map it to your data types, Parquet4s comes with a handy mechanism that derives it automatically from Scala case classes. Please follow this documentation to learn which Scala types are supported out of the box and how to define custom encoders and decoders. If you do not wish to map the schema of your data to Scala case classes, then Parquet4s allows you to stick to generic records, that is, to the aforementioned subtypes of ParquetRecord. Still, you must provide MessageType during writing. If you do not provide it during reading, then Parquet4s uses the schema stored in a file, and all its content is read. Supported types Primitive types Type Reading and Writing Filtering Int ☑ ☑ Long ☑ ☑ Byte ☑ ☑ Short ☑ ☑ Boolean ☑ ☑ Char ☑ ☑ Float ☑ ☑ Double ☑ ☑ BigDecimal ☑ ☑ java.time.LocalDateTime [*with INT96] ☑ ☒ java.time.LocalDateTime [*with INT64] ☑ ☑ java.time.Instant [*with INT96] ☑ ☒ java.time.Instant [*with INT64] ☑ ☑ java.time.LocalDate ☑ ☑ java.sql.Timestamp [*with INT96] ☑ ☒ java.sql.Timestamp [*with INT64] ☑ ☑ java.sql.Date ☑ ☑ Array[Byte] ☑ ☑ *) You can change the default format of the timestamp column from INT96 to INT64 by importing type classes: INT64 micros format: import com.github.mjakubowski84.parquet4s.TimestampFormat.Implicits.Micros._ INT64 mills format: import com.github.mjakubowski84.parquet4s.TimestampFormat.Implicits.Millis._ INT64 nanos format: import com.github.mjakubowski84.parquet4s.TimestampFormat.Implicits.Nanos._ The imports contain type classes supporting projection, filtering and writing. Complex Types Complex types can be arbitrarily nested. Option List Seq Vector Set Array - An array of bytes is treated as primitive binary Map - Key must be of primitive type, only the immutable version. Any Scala collection that has Scala collection Factory (in 2.12 it is derived from CanBuildFrom). Refers to both mutable and immutable collections. Collection must be bounded only by one type of element - because of that Map is supported only in the immutable version. Any case class Custom Types Parquet4s is built using Scala’s type class system. That allows you to extend Parquet4s by defining your own implementations of type classes. For example, you may define a codec for your own type so that it can be read from or written to Parquet. Assuming that you have your own type: case class CustomType(i: Int) You want to save it as optional Int. In order to achieve that you have to define a codec: import com.github.mjakubowski84.parquet4s.{OptionalValueCodec, IntValue, Value, ValueCodecConfiguration} case class CustomType(i: Int) implicit val customTypeCodec: OptionalValueCodec[CustomType] = new OptionalValueCodec[CustomType] { override protected def decodeNonNull(value: Value, configuration: ValueCodecConfiguration): CustomType = value match { case IntValue(i) =&gt; CustomType(i) } override protected def encodeNonNull(data: CustomType, configuration: ValueCodecConfiguration): Value = IntValue(data.i) } ValueCodec composes ValueEncoder and ValueDecoder, so if you need only to read or only to write your type, then it is enough if you implement only one of them. Additionally, if you want to write your custom type, you have to define the schema for it: import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType} import com.github.mjakubowski84.parquet4s.TypedSchemaDef import com.github.mjakubowski84.parquet4s.{LogicalTypes, SchemaDef} case class CustomType(i: Int) implicit val customTypeSchema: TypedSchemaDef[CustomType] = SchemaDef.primitive( primitiveType = PrimitiveType.PrimitiveTypeName.INT32, required = false, logicalTypeAnnotation = Option(LogicalTypes.Int32Type) ).typed[CustomType] In order to filter by a field of a custom type T you have to implement FilterCodec[T] type class. import com.github.mjakubowski84.parquet4s.FilterCodec import org.apache.parquet.filter2.predicate.Operators.IntColumn case class CustomType(i: Int) implicit val customFilterCodec: FilterCodec[CustomType, java.lang.Integer, IntColumn] = FilterCodec[CustomType, java.lang.Integer, IntColumn]( encode = (customType, _) =&gt; customType.i, decode = (integer, _) =&gt; CustomType(integer) ) Using generic records directly Parquet4s allows you to choose to use generic records explicitly from the level of API in each module of the library. But you can also use typed API and define RowParquetRecord as your data type. Parquet4s contains type classes for encoding, decoding and schema provisioning for RowParquetRecord. import com.github.mjakubowski84.parquet4s.{ParquetReader, ParquetWriter, Path, RowParquetRecord} import org.apache.parquet.schema.MessageType // both reads are equivalent ParquetReader.generic.read(Path(\"file.parquet\")) ParquetReader.as[RowParquetRecord].read(Path(\"file.parquet\")) val data: Iterable[RowParquetRecord] = ??? // when using generic record you need to define the schema on your own implicit val schema: MessageType = ??? // both writes are equivalent ParquetWriter .generic(schema) // schema is passed explicitly .writeAndClose(Path(\"file.parquet\"), data) ParquetWriter .of[RowParquetRecord] // schema is passed implicitly .writeAndClose(Path(\"file.parquet\"), data)"
    } ,      
    {
      "title": "Distinguished Sponsors",
      "url": "/parquet4s/docs/sponsors/",
      "content": "Distinguished Sponsors calvinlfer"
    } ,    
    {
      "title": "Statistics",
      "url": "/parquet4s/docs/statistics/",
      "content": "Statistics Parquet files contain metadata that are used to optimize filtering. Additionally, Parquet4s leverages metadata to provide insight about datasets in an efficient way: Number of records Min value of a column Max value of a column Parquet4s will try to resolve those statistics without iterating over each record if possible. Statistics can also be queried using a filter — but please mind that speed of the query might decrease as, due to filtering, the algorithm might need to iterate over the content of a row group to resolve min/max values. The performance of the query is the best in the case of sorted datasets. Parquet4s provides separate API for Statistics. It is also leveraged in ParqueIterablee.g. to efficiently calculate size. import com.github.mjakubowski84.parquet4s.{Col, Path, Stats} import java.time.LocalDate case class User(id: Long, age: Int, registered: LocalDate) // stats of users that registered in year 2020 val userStats = Stats .builder .filter(Col(\"registered\") &gt;= LocalDate.of(2020, 1, 1) &amp;&amp; Col(\"registered\") &lt; LocalDate.of(2021, 1, 1)) .projection[User] .stats(Path(\"users\")) val numberOfUsers = userStats.recordCount val minAge = userStats.min[Int](Col(\"age\")) val maxAge = userStats.max[Int](Col(\"age\")) import com.github.mjakubowski84.parquet4s.{Col, ParquetReader, Path, Stats} import java.time.LocalDate case class User(id: Long, age: Int, registered: LocalDate) // users that registered in year 2020 val users = ParquetReader .projectedAs[User] .filter(Col(\"registered\") &gt;= LocalDate.of(2020, 1, 1) &amp;&amp; Col(\"registered\") &lt; LocalDate.of(2021, 1, 1)) .read(Path(\"users\")) try { val numberOfUsers = users.size val minAge = users.min[Int](Col(\"age\")) val maxAge = users.max[Int](Col(\"age\")) } finally { users.close() }"
    } ,    
    {
      "title": "Supported storage types",
      "url": "/parquet4s/docs/storage_types/",
      "content": "Supported storage types As it is based on Hadoop Client, Parquet4s can read and write from a variety of file systems: Local files HDFS Amazon S3 Google Storage Azure Blob Storage Azure Data Lake Storage OpenStack Swift and any other storage compatible with Hadoop… Please refer to Hadoop Client documentation or your storage provider to check how to connect to your storage."
    }    
  ];

  idx = lunr(function () {
    this.ref("title");
    this.field("content");

    docs.forEach(function (doc) {
      this.add(doc);
    }, this);
  });

  docs.forEach(function (doc) {
    docMap.set(doc.title, doc.url);
  });
}

// The onkeypress handler for search functionality
function searchOnKeyDown(e) {
  const keyCode = e.keyCode;
  const parent = e.target.parentElement;
  const isSearchBar = e.target.id === "search-bar";
  const isSearchResult = parent ? parent.id.startsWith("result-") : false;
  const isSearchBarOrResult = isSearchBar || isSearchResult;

  if (keyCode === 40 && isSearchBarOrResult) {
    // On 'down', try to navigate down the search results
    e.preventDefault();
    e.stopPropagation();
    selectDown(e);
  } else if (keyCode === 38 && isSearchBarOrResult) {
    // On 'up', try to navigate up the search results
    e.preventDefault();
    e.stopPropagation();
    selectUp(e);
  } else if (keyCode === 27 && isSearchBarOrResult) {
    // On 'ESC', close the search dropdown
    e.preventDefault();
    e.stopPropagation();
    closeDropdownSearch(e);
  }
}

// Search is only done on key-up so that the search terms are properly propagated
function searchOnKeyUp(e) {
  // Filter out up, down, esc keys
  const keyCode = e.keyCode;
  const cannotBe = [40, 38, 27];
  const isSearchBar = e.target.id === "search-bar";
  const keyIsNotWrong = !cannotBe.includes(keyCode);
  if (isSearchBar && keyIsNotWrong) {
    // Try to run a search
    runSearch(e);
  }
}

// Move the cursor up the search list
function selectUp(e) {
  if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index) && (index > 0)) {
      const nextIndexStr = "result-" + (index - 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Move the cursor down the search list
function selectDown(e) {
  if (e.target.id === "search-bar") {
    const firstResult = document.querySelector("li[id$='result-0']");
    if (firstResult) {
      firstResult.firstChild.focus();
    }
  } else if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index)) {
      const nextIndexStr = "result-" + (index + 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Search for whatever the user has typed so far
function runSearch(e) {
  if (e.target.value === "") {
    // On empty string, remove all search results
    // Otherwise this may show all results as everything is a "match"
    applySearchResults([]);
  } else {
    const tokens = e.target.value.split(" ");
    const moddedTokens = tokens.map(function (token) {
      // "*" + token + "*"
      return token;
    })
    const searchTerm = moddedTokens.join(" ");
    const searchResults = idx.search(searchTerm);
    const mapResults = searchResults.map(function (result) {
      const resultUrl = docMap.get(result.ref);
      return { name: result.ref, url: resultUrl };
    })

    applySearchResults(mapResults);
  }

}

// After a search, modify the search dropdown to contain the search results
function applySearchResults(results) {
  const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
  if (dropdown) {
    //Remove each child
    while (dropdown.firstChild) {
      dropdown.removeChild(dropdown.firstChild);
    }

    //Add each result as an element in the list
    results.forEach(function (result, i) {
      const elem = document.createElement("li");
      elem.setAttribute("class", "dropdown-item");
      elem.setAttribute("id", "result-" + i);

      const elemLink = document.createElement("a");
      elemLink.setAttribute("title", result.name);
      elemLink.setAttribute("href", result.url);
      elemLink.setAttribute("class", "dropdown-item-link");

      const elemLinkText = document.createElement("span");
      elemLinkText.setAttribute("class", "dropdown-item-link-text");
      elemLinkText.innerHTML = result.name;

      elemLink.appendChild(elemLinkText);
      elem.appendChild(elemLink);
      dropdown.appendChild(elem);
    });
  }
}

// Close the dropdown if the user clicks (only) outside of it
function closeDropdownSearch(e) {
  // Check if where we're clicking is the search dropdown
  if (e.target.id !== "search-bar") {
    const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
    if (dropdown) {
      dropdown.classList.remove("show");
      document.documentElement.removeEventListener("click", closeDropdownSearch);
    }
  }
}
