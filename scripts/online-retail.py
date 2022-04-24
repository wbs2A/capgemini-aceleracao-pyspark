from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# REGEX PATTERNS ---
REGEX_ALPHA = r'[a-zA-Z]+'
REGEX_EMPTY_STR = r'[\t ]+$'
REGEX_INTEGER = r'[0-9]+'
REGEX_INVOICENO = r'c?([0-9]{6})'


# --------------------

# Helper functions ---
def check_empty_column(col):
    return F.col(col).isNull() | (F.col(col) == '') | F.col(col).rlike(REGEX_EMPTY_STR)


# perform the quality asurence on the received dataframe
def ensures_quality(dataframe):
    data = dataframe.withColumn('qa_InvoiceNo', (
        F.when(
            check_empty_column('InvoiceNo'), 'M'
        ).when(
            (F.length(F.col('InvoiceNo')) != 6), 'S'
        ).when(
            (F.col('InvoiceNo').rlike(REGEX_INVOICENO)), F.col('InvoiceNo')
        ).otherwise('F')
    ))

    # ensures StockCode
    data = data.withColumn('qa_StockCode', (
        F.when(
            check_empty_column('StockCode'), 'M'
        ).when(
            (F.length(F.col('StockCode')) != 5), 'S'
        ).when(
            (F.col('StockCode').rlike(REGEX_INTEGER) & (F.length(F.col('StockCode')) == 5)),
            F.col('StockCode').cast(IntegerType())
        ).otherwise('F')
    ))

    # ensures Description
    data = data.withColumn('qa_Description', (
        F.when(
            check_empty_column('Description'), 'M'
        ).otherwise(
            F.col('Description')
        )
    ))

    data = data.withColumn('qa_Quantity', (
        F.when(
            check_empty_column('Quantity'), 'M'
        ).when(
            (~F.col('Quantity').rlike(REGEX_INTEGER)), 'F'
        ).otherwise(
            F.col('Quantity')
        )
    ))

    data = data.withColumn('qa_UnitPrice', (
        F.when(
            check_empty_column('UnitPrice'), 'M'
        ).when(
            F.col('UnitPrice').contains('-'), 'I'
        ).otherwise(
            F.col('UnitPrice')
        )
    ))

    data = data.withColumn('qa_CustomerID', (
        F.when(
            check_empty_column('CustomerID'), 'M'
        ).when(
            (F.length(F.col('CustomerID')) == 5), 'S'
        ).when(
            (F.col('CustomerID').rlike(REGEX_INTEGER) & (F.length(F.col('CustomerID')) == 5)),
            F.col('StockCode').cast(IntegerType())
        )
    ))

    data = data.withColumn('qa_Country', (
        F.when(
            check_empty_column('Country'), 'M'
        ).otherwise(F.col('Country'))
    ))

    return data


def transform_df(dataframe):
    """
    disclaimer: This function performs transformation on the dataset (i.e. treat the uncertain values in the columns).
    All the transformations that were realized here is mapped by me, and it can change with my perspective on the
    problems
    """
    data = dataframe.withColumn('UnitPrice', (
        F.when(
            (check_empty_column('UnitPrice') | (F.col('UnitPrice').contains('-'))), 0.0
        ).when(
            F.col('UnitPrice').contains(','), F.regexp_replace('UnitPrice', ',', '.').cast(FloatType())
        ).otherwise(
            F.col('UnitPrice').cast(FloatType())
        )
    ))

    data = data.withColumn('InvoiceDate', (
        F.when(
            check_empty_column('InvoiceDate'), ''
        ).otherwise(
            F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy HH:mm').cast(TimestampType())
        )
    ))

    # ensures the Quantity
    data = data.withColumn('Quantity', (
        F.when(
            check_empty_column('Quantity'), 0
        ).otherwise(
            F.col('Quantity').cast(IntegerType())
        )
    ))
    return data


def clean_dataframe(dataframe):
    data = dataframe.select('*').where(dataframe['StockCode'] != 'PADS')
    data = ensures_quality(data)
    data = transform_df(data)

    data.show(20)

    return data


# --------------------

# Bussiness questions to Online Retail
def question1(dataframe):
    data = dataframe.filter(F.col('StockCode').contains('gift_0001'))
    results = data.agg(F.format_number(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2))
    results.show()


def question2(dataframe):
    data = dataframe.filter(F.col('StockCode').contains('gift_0001'))
    results = data.groupBy(F.month('InvoiceDate').alias('Mês')).agg(F.format_number(F.sum('UnitPrice'), 2)).orderBy(
        'Mês')
    results.show()


def question3(dataframe):
    data = dataframe.filter(F.col('StockCode').endswith('S'))
    results = data.agg(F.sum(F.col('Quantity')))
    results.show()


def question4(dataframe):
    data = dataframe.groupBy('StockCode') \
        .agg(
        F.sum('Quantity').alias('soma')
    ).orderBy(F.desc('soma'))
    data.show(1)


def question5(dataframe):
    data = dataframe.groupBy(F.col('StockCode'), F.month('InvoiceDate').alias('month')) \
        .agg(F.sum('Quantity').alias('soma')) \
        .orderBy(F.desc('soma'))
    data.show()


def question6(dataframe):
    data = dataframe.groupBy(F.hour('InvoiceDate')) \
        .agg(F.format_number(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('valor')) \
        .orderBy(F.col('valor').desc()) \
        .limit(1)
    data.show()


def question7(dataframe):
    data = dataframe.groupBy(F.month('InvoiceDate')) \
        .agg(F.format_number(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('valor')) \
        .orderBy(F.col('valor').desc()) \
        .limit(1)
    data.show()


def question8(dataframe):
    most_sales_year = dataframe.groupBy(F.year('InvoiceDate').alias('year')) \
        .agg(F.format_number(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('valor')) \
        .orderBy(F.col('valor').desc()) \
        .limit(1) \
        .first()['year']

    data = dataframe.where(F.year('InvoiceDate') == most_sales_year) \
        .groupBy(F.col('Description'), F.month('InvoiceDate')) \
        .agg(F.format_number(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('valor')) \
        .orderBy(F.col('valor').desc()) \
        .limit(1)
    data.show()


def question9(dataframe):
    data = dataframe.groupBy(F.col('Country')) \
        .agg(F.format_number(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('valor')) \
        .orderBy(F.col('valor').desc()) \
        .limit(1)
    data.show()


def question10(dataframe):
    data = dataframe.where(F.col('StockCode').endswith('M')) \
        .groupBy(F.col('Country')) \
        .agg(F.format_number(F.sum(F.col('Quantity')), 2).alias('valor')) \
        .orderBy(F.col('valor').desc()) \
        .limit(1)
    data.show()


def question11(dataframe):
    data = dataframe.groupBy(F.col('InvoiceNo')) \
        .agg(F.max(F.col('Quantity') * F.col('UnitPrice')).alias('greatest_sell_value')) \
        .orderBy(F.col('greatest_sell_value').desc()) \
        .limit(1)
    data.show()


def question12(dataframe):
    data = dataframe.groupBy(F.col('InvoiceNo')) \
        .agg(F.max(F.col('Quantity')).alias('greatest_quantity')) \
        .orderBy(F.col('greatest_quantity').desc()) \
        .limit(1)
    data.show()


def question13(dataframe):
    data = dataframe.where(F.col('CustomerID').isNotNull()) \
                            .groupBy(F.col('CustomerID')) \
                            .count() \
                            .orderBy(F.col('count').desc()) \
                            .limit(1)
    data.show()


def solve_questions(dataframe, method_name, _method):
    print("Questão", method_name[8:])
    _method(dataframe)


# -------------------------------------

if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

    schema_online_retail = StructType([
        StructField('InvoiceNo',   StringType(), True),
        StructField('StockCode',   StringType(), True),
        StructField('Description', StringType(), True),
        StructField('Quantity',    StringType(), True),
        StructField('InvoiceDate', StringType(), True),
        StructField('UnitPrice',   StringType(), True),
        StructField('CustomerID',  StringType(), True),
        StructField('Country',     StringType(), True)
    ])

    df = (spark.getOrCreate().read
          .format("csv")
          .option("header", "true")
          .schema(schema_online_retail)
          .load("data/online-retail/online-retail.csv"))

    df.printSchema()

    treated_df = clean_dataframe(df)

    treated_df.printSchema()

    online_retail_questions = {
        "question1": question1,
        "question2": question2,
        "question3": question3,
        "question4": question4,
        "question5": question5,
        "question6": question6,
        "question7": question7,
        "question8": question8,
        "question9": question9,
        "question10": question10,
        "question11": question11,
        "question12": question12,
        "question13": question13
    }

    for _name, _method in online_retail_questions.items():
        solve_questions(treated_df, _name, _method)
