# This is a sample Python script.
"""
CSV to SQLite Database Converter

This module provides functionality to read CSV files and convert them
to SQLite database tables with appropriate column types.
"""


# Third-party imports
import pandas as pd
from pandas import DataFrame

# SQLAlchemy imports
from sqlalchemy import (
    String, Integer, Float, Boolean, DateTime,
    Text, Date, Column, create_engine
)
from sqlalchemy.orm import declarative_base, sessionmaker

# Constants and Configurations
DATABASE_URL = 'sqlite:///example.db'
DEFAULT_STRING_LENGTH = 255

# Create the database engine at module level
engine = create_engine(DATABASE_URL, echo=True)

# Create the base class for dynamic models
DynamicBase = declarative_base()



def get_sqlalchemy_type(pandas_type):
    """Map pandas dtypes to SQLAlchemy types"""
    # Convert pandas type to string for easier comparison
    type_str = str(pandas_type)

    if 'int' in type_str:
        return Integer
    elif 'float' in type_str:
        return Float
    elif 'bool' in type_str:
        return Boolean
    elif 'datetime' in type_str:
        return DateTime
    elif 'date' in type_str:
        return Date
    elif 'object' in type_str:  # Usually strings in pandas
        return String(length=255)  # You might want to adjust the length
    else:
        return Text  # Default to Text for any other types


# Generate the model class dynamically based on your DataFrame
def create_table_class(df, table_name):
    class_attrs = {
        '__tablename__': table_name,
        'id': Column(Integer, primary_key=True)
    }

    # Get the data types DataFrame
    dtype_df = pd.DataFrame(df.dtypes).reset_index()
    dtype_df.columns = ['Column', 'Dtype']

    # Create columns based on the DataFrame's structure
    for _, row in dtype_df.iterrows():
        column_name = row['Column']
        pandas_type = row['Dtype']

        # Skip if column name is 'id' as we already defined it
        if column_name.lower() != 'id':
            sqlalchemy_type = get_sqlalchemy_type(pandas_type)
            class_attrs[column_name] = Column(sqlalchemy_type)

    # Create the new class dynamically
    return type(table_name, (DynamicBase,), class_attrs)


def read_file(file_name: str) -> None:
    #boolean to track if file is found
    file_is_found = False


    while file_is_found == False:
        # Read the file
        try:
            with open(f"{file_name}", "r") as file:
                content = file.read()
                print(content)
        #handle errors
        except FileNotFoundError:
            print("Error: The specified file was not found.")
            file_name = input("Enter file name/location or 0 to exit script: ")
            if(file_name == '0'):
                break
            else:
                continue
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        else:
            file_is_found = True
            df = pd.read_csv(f"{file_name}")
            preview_save_data(df)


# Data preview and storage functions
def preview_save_data(df: DataFrame) -> None:
    """Preview data and save to database if confirmed."""
    data_extracted = df[:]
    print(data_extracted.head())
    data_extracted.info()
    print(df.describe())

    if input("Should this be saved to the database? (yes/no): ").upper() == 'YES':
        save_to_database(data_extracted)
    else:
        quit()


def save_to_database(data_extracted: DataFrame) -> None:
    """Save DataFrame to database."""
    try:
        DynamicTable = create_table_class(data_extracted, 'dynamic_table')
        DynamicBase.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            for record in data_extracted.to_dict('records'):
                session.add(DynamicTable(**record))
            session.commit()
            print("Data successfully saved to database!")
        except Exception as e:
            session.rollback()
            print(f"Error saving to database: {e}")
        finally:
            session.close()
    except Exception as e:
        print(f"Error setting up database: {e}")


def main():
        global engine, file_name
        # Ask for file name to import
        file_name = input("What is the file name/location to import? ")
        read_file(file_name)

if __name__ == '__main__':
    main()




