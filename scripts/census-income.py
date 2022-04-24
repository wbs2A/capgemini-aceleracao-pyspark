from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def show_references():
    print("""
    | This data was extracted from the census bureau database found at
    | https://www.census.gov/ftp/pub/DES/www/welcome.html
    | Donor: Ronny Kohavi and Barry Becker,
    |        Data Mining and Visualization
    |        Silicon Graphics.
    |        e-mail: ronnyk@sgi.com for questions.
    """)


def question1(dataframe):
    data = dataframe.where(~F.col('workclass').contains("?")) \
        .filter(F.col('income').contains("<=50K")) \
        .groupBy(F.col('workclass')) \
        .agg(F.max(F.col('hours-per-week'))) \
        .select(F.col('workclass'))
    data.show()


def question2(dataframe):
    data = dataframe.groupBy(F.col('race')) \
        .agg(F.format_number(F.avg(F.col('hours-per-week')), 2).alias('avg_pay_for_week')) \
        .orderBy(F.col('avg_pay_for_week').desc()) \
        .select(F.col('race'), F.col('avg_pay_for_week'))
    data.show()


def genderproportion(dataframe, gender):
    total_rows = dataframe.count()
    total_male = dataframe.where(~F.col('sex').contains("?")) \
        .filter(F.col('sex') == ' Male') \
        .count()
    total_female = dataframe.where(~F.col('sex').contains("?")) \
        .filter(F.col('sex') == ' Female') \
        .count()
    print('Total rows: ', total_rows)
    print('Total male: ', total_male)
    print('Total female: ', total_female)

    if gender == 'Male':
        return total_male / total_female
    return total_female / total_male


def question3(dataframe):
    results = genderproportion(dataframe, 'Male')
    print(f"Proportion of male/female {results:.2f}\n")


def question4(dataframe):
    results = genderproportion(dataframe, 'Female')
    print(f"Proportion of male/female {results:.2f}\n")


def question5(dataframe):
    data = dataframe.where(~F.col('occupation').contains("?")) \
        .groupBy(F.col('occupation')) \
        .agg(F.format_number(F.avg(F.col('hours-per-week')), 2).alias('max_avg_hours')) \
        .orderBy(F.col('max_avg_hours')) \
        .limit(1)
    data.show()


def question6(dataframe):
    data = dataframe.where(~F.col('occupation').contains("?")) \
        .filter(~F.col('education').contains("?")) \
        .groupBy(F.col('education'), F.col('occupation')) \
        .agg(F.count('occupation').alias('occurences')) \
        .orderBy(F.col('occurences').desc()) \
        .select('*')
    print('ranking the results')
    data.show()
    print("-" * 30)

    data = data.groupBy(F.col('education')) \
        .agg(F.first(F.col('occupation').alias('most_commom_occupation')))

    print('most commom ocupation')
    data.show()


def question7(dataframe):
    data = dataframe.groupBy(F.col('sex'), F.col('occupation')) \
        .agg(F.count('occupation').alias('occurences')) \
        .orderBy(F.col('occurences').desc())
    print('ranking the results')
    data.show()
    print("-" * 30)

    data = data.groupBy(F.col('sex')) \
        .agg(F.first('occupation').alias('most_commom_occupation'))

    print('most commom ocupation')
    data.show()


def question8(dataframe):
    data = dataframe.groupBy(F.col('race'), F.col('education')) \
        .agg(F.count('education').alias('occurences')) \
        .orderBy(F.col('occurences').desc())
    print('ranking the results')
    data.show()
    print("-" * 30)
    data = data.groupBy(F.col('race')) \
        .agg(F.first('education').alias('highest_education_per_race'))
    print('highest education per race')
    data.show()


def question9(dataframe):
    data = dataframe.withColumn('self-employed', (
        F.when(F.col('workclass').rlike('Self-emp-not-inc'), 'self-employed').when(
            F.col('workclass').rlike(' Self-emp-inc'), 'self-employed')
    ))
    data.where(F.col('self-employed').rlike('self-employed')) \
        .groupBy(F.col('education'), F.col('sex'), F.col('race')) \
        .agg(F.count(F.col('self-employed')).alias('count')) \
        .orderBy(F.col('count').desc()) \
        .limit(1) \
        .show()
    # check each result
    most_commom_ed = \
        dataframe.filter(F.col('workclass').rlike('Self-emp-not-inc') |
                         F.col('workclass').rlike('Self-emp-inc')) \
                 .groupBy(F.col('education')) \
                 .agg(F.count(F.col('education')).alias('count_ed')) \
                 .orderBy(F.col('count_ed').desc()) \
                 .first()['education']
    print(most_commom_ed)

    most_commom_race = \
        dataframe.filter(F.col('workclass').rlike('Self-emp-not-inc') |
                         F.col('workclass').rlike('Self-emp-inc')) \
                 .groupBy(F.col('race')) \
                 .agg(F.count(F.col('race')).alias('count_race')) \
                 .orderBy(F.col('count_race').desc()) \
                 .first()['race']
    print(most_commom_race)

    most_commom_sex = \
        dataframe.filter(F.col('workclass').rlike('Self-emp-not-inc') |
                         F.col('workclass').rlike('Self-emp-inc')) \
                 .groupBy(F.col('sex')) \
                 .agg(F.count(F.col('sex')).alias('count_sex')) \
                 .orderBy(F.col('count_sex').desc()) \
                 .first()['sex']
    print(most_commom_sex)


def question10(dataframe):
    data = dataframe.withColumn('married_status', (
        F.when(F.col('marital-status').rlike('Married-civ-spouse'), 'married').when(
            F.col('marital-status').rlike('Married-AF'), 'married').when(
            F.col('marital-status').rlike('Married-spouse-absent'), 'married').otherwise('not_married')
    ))

    total_married = data.filter(F.col('married_status') == 'married') \
        .count()
    total_unmarried = data.filter(F.col('married_status') == 'not_married') \
        .count()
    print(f'A razão entre casados e não casados é {total_married / total_unmarried:.2f}\n')


def question11(dataframe):
    data = dataframe.withColumn('married_status', (
        F.when(F.col('marital-status').rlike('Married-civ-spouse'), 'married').when(
            F.col('marital-status').rlike('Married-AF'), 'married').when(
            F.col('marital-status').rlike('Married-spouse-absent'), 'married').otherwise('not_married')
    ))

    data.filter(F.col('married_status') == 'not_married') \
        .groupBy(F.col('race')) \
        .agg(F.count(F.col('race')).alias('count_race')) \
        .orderBy(F.col('count_race').desc()) \
        .limit(2) \
        .show()


def question12(dataframe):
    data = dataframe.withColumn('married_status', (
        F.when(F.col('marital-status').rlike('Married-civ-spouse'), 'married').when(
            F.col('marital-status').rlike('Married-AF'), 'married').when(
            F.col('marital-status').rlike('Married-spouse-absent'), 'married').otherwise('not_married')
    ))
    data.filter(F.col('married_status') == 'married') \
        .groupBy(F.col('married_status'), F.col('income')) \
        .agg(F.count(F.col('income')).alias('count_income')) \
        .orderBy(F.col('count_income').desc()) \
        .limit(1) \
        .show()

    data.filter(F.col('married_status') == 'not_married') \
        .groupBy(F.col('married_status'), F.col('income')) \
        .agg(F.count(F.col('income')).alias('count_income')) \
        .orderBy(F.col('count_income').desc()) \
        .limit(1) \
        .show()


def question13(dataframe):
    data = dataframe.where(~F.col('sex').contains('?')) \
        .groupBy(F.col('sex'), F.col('income')) \
        .agg(F.count(F.col('income')).alias('income_occurence')) \
        .orderBy(F.col('income_occurence').desc()) \
        .select('*')

    data = data.groupBy(F.col('sex')) \
        .agg(F.first('income').alias('most_commom_income'))

    data.show()


def question14(dataframe):
    data = dataframe.where(~F.col('native-country').contains('?')) \
        .groupBy(F.col('native-country'), F.col('income')) \
        .agg(F.count(F.col('income')).alias('income_occurence')) \
        .orderBy(F.col('income_occurence').desc()) \
        .select('*')
    data = data.groupBy(F.col('native-country')) \
        .agg(F.first('income').alias('most_commom_income'))
    data.show()


def question15(dataframe):
    caucasian_occurence = dataframe.filter(F.col('race').rlike('White')) \
        .count()
    non_caucasian_ocurr = dataframe.filter(~F.col('race').rlike('White')) \
        .count()
    print(f"A razão entre pessoas brancas e não brancas é: {caucasian_occurence / non_caucasian_ocurr:.2f}")


def solve_questions(dataframe, method_name, _method):
    print("Questão", method_name[8:])
    _method(dataframe)


if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

    schema_census_income = StructType([
        StructField('age', IntegerType(), True),
        StructField('workclass', StringType(), True),
        StructField('fnlwgt', FloatType(), True),
        StructField('education', StringType(), True),
        StructField('education-num', IntegerType(), True),
        StructField('marital-status', StringType(), True),
        StructField('occupation', StringType(), True),
        StructField('relationship', StringType(), True),
        StructField('race', StringType(), True),
        StructField('sex', StringType(), True),
        StructField('capital-gain', FloatType(), True),
        StructField('capital-loss', FloatType(), True),
        StructField('hours-per-week', FloatType(), True),
        StructField('native-country', StringType(), True),
        StructField('income', StringType(), True),
    ])

    df = (spark.getOrCreate().read
          .format("csv")
          .option("header", "true")
          .schema(schema_census_income)
          .load("data/census-income/census-income.csv"))

    df.printSchema()

    df.show(10)

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
        "question13": question13,
        "question14": question14,
        "question15": question15
    }

    for i, (_name, _method) in enumerate(online_retail_questions.items()):
        print("Questão", i + 1)
        _method(df)
    print('-' * 15, 'end of tasks', '-' * 15)
    show_references()
