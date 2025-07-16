import pytest
import pandas as pd
import numpy as np
from sqlalchemy import String, Integer, Float, Boolean, DateTime, Text, Date
import os
from tempfile import NamedTemporaryFile
from unittest.mock import patch, MagicMock
from read_csv_example import (
    get_sqlalchemy_type,
    create_table_class,
    get_model_structure,
    read_file,
    preview_save_data
)

@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing"""
    data = {
        'int_col': [1, 2, 3],
        'float_col': [1.1, 2.2, 3.3],
        'str_col': ['a', 'b', 'c'],
        'bool_col': [True, False, True],
    }
    return pd.DataFrame(data)

@pytest.fixture
def temp_csv_file(sample_dataframe):
    """Create a temporary CSV file for testing"""
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_dataframe.to_csv(f.name, index=False)
        temp_file_name = f.name
    yield temp_file_name
    os.unlink(temp_file_name)

def test_get_sqlalchemy_type():
    """Test mapping of pandas types to SQLAlchemy types"""
    test_cases = [
        (np.dtype('int64'), Integer),
        (np.dtype('float64'), Float),
        (np.dtype('bool'), Boolean),
        (np.dtype('datetime64[ns]'), DateTime),
        (np.dtype('object'), String),
    ]
    
    for pandas_type, expected_type in test_cases:
        result = get_sqlalchemy_type(pandas_type)
        if isinstance(expected_type(), String):
            assert isinstance(result, type(String()))
        else:
            assert result == expected_type

def test_create_table_class(sample_dataframe):
    """Test creation of dynamic table class"""
    DynamicTable = create_table_class(sample_dataframe, 'test_table')
    
    # Check table name
    assert DynamicTable.__tablename__ == 'test_table'
    
    # Check columns
    expected_columns = {'id', 'int_col', 'float_col', 'str_col', 'bool_col'}
    actual_columns = set(DynamicTable.__table__.columns.keys())
    assert actual_columns == expected_columns

def test_get_model_structure(sample_dataframe):
    """Test model structure extraction"""
    with patch('builtins.print') as mock_print:
        get_model_structure(sample_dataframe)
        mock_print.assert_called_once()

@pytest.mark.parametrize('user_input', ['0', 'valid_file.csv'])
def test_read_file_not_found(user_input):
    """Test file not found handling"""
    with patch('builtins.input', return_value=user_input):
        with patch('builtins.print') as mock_print:
            read_file('nonexistent_file.csv')
            mock_print.assert_called_with("Error: The specified file was not found.")

def test_read_file_success(temp_csv_file):
    """Test successful file reading"""
    with patch('read_csv_example.preview_save_data') as mock_preview:
        read_file(temp_csv_file)
        mock_preview.assert_called_once()

@pytest.mark.parametrize('save_option', ['yes', 'no', 'YES', 'NO'])
def test_preview_save_data(sample_dataframe, save_option):
    """Test preview and save functionality"""
    with patch('builtins.input', return_value=save_option):
        with patch('builtins.print') as mock_print:
            if save_option.upper() == 'NO':
                with pytest.raises(SystemExit):
                    preview_save_data(sample_dataframe)
            else:
                # Mock database-related objects
                mock_engine = MagicMock()
                mock_session = MagicMock()
                with patch('read_csv_example.engine', mock_engine):
                    with patch('sqlalchemy.orm.sessionmaker', return_value=lambda: mock_session):
                        preview_save_data(sample_dataframe)
                        mock_session.commit.assert_called_once()

def test_preview_save_data_error_handling(sample_dataframe):
    """Test error handling in preview_save_data"""
    with patch('builtins.input', return_value='YES'):
        mock_session = MagicMock()
        mock_session.commit.side_effect = Exception("Database error")
        
        with patch('sqlalchemy.orm.sessionmaker', return_value=lambda: mock_session):
            with patch('builtins.print') as mock_print:
                preview_save_data(sample_dataframe)
                mock_print.assert_any_call("Error saving to database: Database error")
                mock_session.rollback.assert_called_once()
                mock_session.close.assert_called_once()