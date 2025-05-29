import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import org.apache.spark.sql.types._
import java.time.Instant

val spark = SparkSession.builder()
  .appName("Customer ETL SCD1 Example")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// === 1. RAW: читаємо з CSV (всі поля будуть StringType) ===

val rawDF = spark.read
  .option("header", true) // Перша стрічка — це заголовки (імена полів)
  .csv("/шлях/до/твого/raw_customer.csv") // <-- зміни шлях на свій

// === 2. STAGING: приводимо типи, додаємо etl_first_load, etl_last_update ===

val utcNow = Instant.now().toString

val stagingDF = rawDF
  .withColumn("id", $"id".cast(IntegerType))
  .withColumn("customer_name", $"customer_name")
  .withColumn("customer_surname", $"customer_surname")
  .withColumn("address", $"address")
  .withColumn("mobile_phone", $"mobile_phone".cast(LongType))
  .withColumn("age", $"age".cast(IntegerType))
  .withColumn("job", $"job")
  .withColumn("etl_first_load", F.lit(utcNow))
  .withColumn("etl_last_update", F.lit(utcNow))

// === 3. SCD1 Merge-функція ===

def scd1_merge(
    targetDF: DataFrame,
    sourceDF: DataFrame,
    keyCols: Seq[String],
    updateCols: Seq[String],
    systemFields: Seq[String]
): DataFrame = {
  val joinExpr = keyCols.map(c => sourceDF(c) === targetDF(c)).reduce(_ && _)
  val joined = targetDF.as("tgt").join(sourceDF.as("src"), joinExpr, "outer")
  joined.select(
    keyCols.map(c => F.coalesce($"src." + c, $"tgt." + c).as(c)) ++
    updateCols.map(c => F.coalesce($"src." + c, $"tgt." + c).as(c)) ++
    systemFields.map(c => F.when($"src." + c.isNotNull, $"src." + c).otherwise($"tgt." + c).as(c))
    : _*
  )
}

// === 4. MART: (імітуємо порожню mart-таблицю для першого прогону) ===

val emptyMartDF = spark.createDataFrame(
  spark.sparkContext.emptyRDD[Row],
  stagingDF.schema
)

// Запуск SCD1 merge: staging → mart
val martDF = scd1_merge(
  targetDF = emptyMartDF,
  sourceDF = stagingDF,
  keyCols = Seq("id"),
  updateCols = Seq("customer_name", "customer_surname", "address", "mobile_phone", "age", "job"),
  systemFields = Seq("etl_first_load", "etl_last_update")
)

// === 5. Подивитися результат (або записати в файл/БД) ===
println("MART:")
martDF.show(false)
