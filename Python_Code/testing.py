import unittest
import pandas as pd


# TEST int

def int_format(df, col):
    """Convertimos la columna en integer format (int64).

    Args:
        df (pd.DataFrame): El dataframe que deseamos modificar.
        col (str): El nombre de la columna a convertir.

    Returns:
        df modificado en formato int

    Raises:
        ValueError: Si la columna no se puede convertir a formato int64.
    """

    try:
        df[col] = df[col].astype(dtype='int64')
        return df
    except (ValueError, TypeError):
        raise ValueError(f"Columna '{col}' no se puede convertir a formato int64")


class TestIntegerFormat(unittest.TestCase):

    def test_existing_integer_column(self):
        """Testeamos para una columna que pueda ser modificada en formato int."""
        data = {'col1': [1.0, 2.0, 3.0]}
        df = pd.DataFrame(data)
        expected_type = 'int64'

        result_df = int_format(df.copy(), 'col1')
        result_type = result_df['col1'].dtype

        self.assertEqual(result_type, expected_type)
        
        data = {'col1': [4, 2, 3]}
        df = pd.DataFrame(data)
        expected_type = 'int64'

        result_df = int_format(df.copy(), 'col1')
        result_type = result_df['col1'].dtype

        self.assertEqual(result_type, expected_type)
    
    
    def test_existing_NO_integer_column(self):
        """Testeamos para una columna que NO pueda ser modificada en formato int."""
        data = {'col1': ['A', 'B', 'C']}
        df = pd.DataFrame(data)
        expected_type = 'int64'

        result_df = int_format(df.copy(), 'col1')
        result_type = result_df['col1'].dtype

        self.assertEqual(result_type, expected_type)



runner = unittest.TextTestRunner()

runner.run(TestIntegerFormat('test_existing_integer_column'))

runner.run(TestIntegerFormat('test_existing_NO_integer_column'))



# TEST flotante



def float_format (df, col):

    """Convertimos la columna en flotante format (float64).

    Args:
        df (pd.DataFrame): El dataframe que deseamos modificar.
        col (str): El nombre de la columna a convertir.

    Returns:
        df modificado en formato float

    Raises:
        ValueError: Si la columna no se puede convertir a formato float64.
    """  


    try:
        df[col] = df[col].astype(dtype='float')
        return df
    except (ValueError, TypeError):
        raise ValueError(f"Columna '{col}' no se puede convertir a formato int64")


class TestFloatFormat(unittest.TestCase):
    def test_float_conversion(self):
        # Create a DataFrame with mixed data types
        data = {'col1': ['1', '2.5', '3'], 'col2': [4, 5.0, 6]}
        df = pd.DataFrame(data)

        # Apply the function to convert the specified column to float
        result_df = float_format(df.copy(), 'col1')

        # Assert that the conversion was successful
        self.assertEqual(result_df['col1'].dtype, 'float64')



    def test_NO_float_conversion(self):
        # Create a DataFrame with mixed data types
        data = {'col1': ['A', 'B', 'C']}
        df = pd.DataFrame(data)

        # Apply the function to convert the specified column to float
        result_df = float_format(df.copy(), 'col1')

        # Assert that the conversion was successful
        self.assertEqual(result_df['col1'].dtype, 'float64')

runner.run(TestFloatFormat('test_float_conversion'))

runner.run(TestFloatFormat('test_NO_float_conversion'))