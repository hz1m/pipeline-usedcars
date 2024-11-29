def validate_dataset(df):
    """Valida o dataset verificando as colunas e tipos básicos."""
    required_columns = {"make", "model", "year", "price", "mileage", "fuel"}
    dataset_columns = set(df.columns)

    # verifica se todas as colunas necessarias estao presentes
    if not required_columns.issubset(dataset_columns):
        print(f"Colunas ausentes: {required_columns - dataset_columns}")
        return False

    return True
