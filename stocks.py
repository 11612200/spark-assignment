from pyspark import SparkContext, SparkConf, conf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    session = SparkSession.builder.appName("Stocks").master("local[*]").getOrCreate()

    stocks = session.read \
        .option("header", "true") \
        .option("inferSchema", value=True) \
        .csv("stock_prices.csv")

    stocks.createOrReplaceTempView("stocksPriceView")

    # stocksAverage = session.sql("SELECT date,AVG((close-open)*volume) AS  ResultAverage FROM stocksPriceView GROUP BY date")
    # stocksAverage.select("date", "ResultAverage").coalesce(1).write.save("averageStockPrice.csv", format="csv",
    #                                                                     header="true")

    stocksMostFrequently = session.sql(
        "SELECT ticker, avg(close*volume) as averageStockPrices from stocksPriceView group by ticker order by averageStockPrices desc limit 1")
    stocksMostFrequently.show()