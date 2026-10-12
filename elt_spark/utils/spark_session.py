from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "AniMind-ETL") -> SparkSession:
    """
    Cria e retorna uma SparkSession configurada com as melhores práticas
    para processamento local de dados no AniMind.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # Usa todos os núcleos de CPU disponíveis
        # Memória e Recursos
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        # Otimização de Shuffle e Partiçoes para ambiente local
        # O padrão do Spark é 200 (muito alto para dados locais), 4-8 é ideal
        .config("spark.sql.shuffle.partitions", "4")
        # Configurações de Parquet
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Padronização de Timezone
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # Suprime logs excessivos do Hadoop/Spark no terminal
    spark.sparkContext.setLogLevel("WARN")

    return spark
