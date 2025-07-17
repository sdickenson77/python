import pytest
import pandas as pd
import numpy as np
from sqlalchemy import String, Integer, Float, Boolean, DateTime, Text

import os
from tempfile import NamedTemporaryFile
from unittest.mock import patch, MagicMock

# Import the functions to test
from read_csv_example import (
    get_sqlalchemy_type,
    create_table_class,
    read_file,
    preview_save_data,
    save_to_database,
)


# Test fixtures
@pytest.fixture
def sample_df_simple():
    """Create a simple DataFrame with basic data types"""
    return pd.DataFrame({
        'number': [1, 2, 3],
        'text': ['a', 'b', 'c']
    })


@pytest.fixture
def sample_df_complex():
    """Create a DataFrame with various data types"""
    return pd.DataFrame({
        'integer': [1, 2, 3],
        'float': [1.1, 2.2, 3.3],
        'text': ['a', 'b', 'c'],
        'boolean': [True, False, True],
        'date': pd.date_range('2023-01-01', periods=3),
        'nullable': [None, 'test', None]
    })


@pytest.fixture
def temp_csv():
    """Create a temporary CSV file"""
    data = "col1,col2\n1,a\n2,b\n3,c"
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(data)
        path = f.name
    yield path
    os.unlink(path)


class TestTypeConversion:
    """Tests for the get_sqlalchemy_type function"""

    @pytest.mark.parametrize("pandas_type,expected_type", [
        (np.dtype('int64'), Integer),
        (np.dtype('int32'), Integer),
        (np.dtype('float64'), Float),
        (np.dtype('bool'), Boolean),
        (np.dtype('datetime64[ns]'), DateTime),
        (np.dtype('object'), String),
    ])
    def test_basic_type_conversion(self, pandas_type, expected_type):
        """Test conversion of basic pandas types to SQLAlchemy types"""
        result = get_sqlalchemy_type(pandas_type)
        if isinstance(result, String):
            assert isinstance(result, type(String()))
        else:
            assert result == expected_type


    def test_string_input_handling(self):
        """Test handling of string input instead of numpy dtype"""
        result = get_sqlalchemy_type('object')
        assert isinstance(result, String)


class TestTableCreation:
    """Tests for the create_table_class function"""

    def test_basic_table_creation(self, sample_df_simple):
        """Test creation of a simple table class"""
        TableClass = create_table_class(sample_df_simple, 'test_table')

        assert TableClass.__tablename__ == 'test_table'
        assert 'id' in TableClass.__table__.columns
        assert 'number' in TableClass.__table__.columns
        assert 'text' in TableClass.__table__.columns

    def test_complex_table_creation(self, sample_df_complex):
        """Test creation of a table with various column types"""
        TableClass = create_table_class(sample_df_complex, 'complex_table')

        assert TableClass.__tablename__ == 'complex_table'
        assert isinstance(TableClass.__table__.columns['integer'].type, Integer)
        assert isinstance(TableClass.__table__.columns['float'].type, Float)
        assert isinstance(TableClass.__table__.columns['boolean'].type, Boolean)
        assert isinstance(TableClass.__table__.columns['date'].type, DateTime)

    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrame"""
        empty_df = pd.DataFrame()
        TableClass = create_table_class(empty_df, 'empty_table')

        assert TableClass.__tablename__ == 'empty_table'
        assert len(TableClass.__table__.columns) == 1  # Only id column
        assert 'id' in TableClass.__table__.columns


class TestFileOperations:
    """Tests for file reading operations"""

    def test_successful_file_read(self, temp_csv):
        """Test successful file reading"""
        with patch('read_csv_example.preview_save_data') as mock_preview:
            read_file(temp_csv)
            mock_preview.assert_called_once()

    def test_file_not_found(self):
        """Test handling of non-existent file"""
        with patch('builtins.input', side_effect=['0']):
            with patch('builtins.print') as mock_print:
                read_file('nonexistent.csv')
                mock_print.assert_any_call("Error: The specified file was not found.")

    def test_file_read_retry(self):
        """Test retry behavior when file is not found"""
        with patch('builtins.input', side_effect=['retry.csv', '0']):
            with patch('builtins.print') as mock_print:
                read_file('nonexistent.csv')
                assert mock_print.call_count >= 2


class TestDataPreviewAndSave:
    """Tests for data preview and save operations"""

    def test_preview_data_yes(self, sample_df_simple):
        """Test preview with 'yes' save option"""
        with patch('builtins.input', return_value='yes'):
            with patch('read_csv_example.save_to_database') as mock_save:
                preview_save_data(sample_df_simple)
                mock_save.assert_called()

    def test_preview_data_no(self, sample_df_simple):
        """Test preview with 'no' save option"""
        with patch('builtins.input', return_value='no'):
            with pytest.raises(SystemExit):
                preview_save_data(sample_df_simple)


    @pytest.mark.parametrize('input_value', ['yes', 'YES', 'Yes'])
    def test_preview_case_insensitive(self, sample_df_simple, input_value):
        """Test case-insensitive input handling"""
        with patch('builtins.input', return_value=input_value):
            with patch('read_csv_example.save_to_database') as mock_save:
                preview_save_data(sample_df_simple)
                mock_save.assert_called_once()


class TestDatabaseOperations:
    def test_save_with_connection_error(self, sample_df_simple):
        """Test handling of database connection error"""
        with patch('read_csv_example.engine') as mock_engine:
            # Make create_all raise an exception to simulate connection error
            mock_engine.create_all.side_effect = Exception("Connection error")

            with patch('builtins.print') as mock_print:
                save_to_database(sample_df_simple)
                mock_print.assert_called_once()