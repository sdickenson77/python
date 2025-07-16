# This is a sample Python script.

#Import pandas library
import pandas as pd
from pandas import DataFrame
# Import the sqlalchemy library
from sqlalchemy import (
    String, Integer, Float, Boolean, DateTime,
    Text, Date, Column, create_engine
)
from sqlalchemy.orm import declarative_base,sessionmaker


def main():
    global engine, file_name
    # Initialize the database
    engine = create_engine('sqlite:///example.db', echo=True)  # `echo=True` logs SQL statements
    # Create the base class for models
    Base = declarative_base()
    # Ask for user's name
    file_name = input("What is the file name/location to import? ")

    read_file(file_name)


def read_file(file_name):
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

def get_model_structure(data_extracted: DataFrame):
    # Get the data types directly as a DataFrame
    dtype_df = pd.DataFrame(data_extracted.dtypes).reset_index()
    dtype_df.columns = ['Column', 'Dtype']  # Rename the columns
    # Print the new DataFrame to verify
    print(dtype_df)

#------------------------------------------------

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


# Create a new Base class for our dynamic model
DynamicBase = declarative_base()


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


def preview_save_data(df: DataFrame):
    # Slice the data
    data_extracted = df[:]
    # Output the resulting dataset
    print(data_extracted.head())
    #data structure
    data_extracted.info()
    # Generate descriptive statistics for numerical columns
    print(df.describe())
    option = input("Should this be saved to the database? (yes/no): ")

    if option.upper() == 'YES':
        try:
            # Get the model structure and create table class
            DynamicTable = create_table_class(data_extracted, 'dynamic_table')
            # Create tables
            DynamicBase.metadata.create_all(engine)

            # Create a session
            Session = sessionmaker(bind=engine)
            session = Session()

            # Convert DataFrame to dict and insert records
            records = data_extracted.to_dict('records')
            for record in records:
                db_record = DynamicTable(**record)
                session.add(db_record)

            # Try to commit and catch any conflicts
            session.commit()
            print("Data successfully saved to database!")

        except Exception as e:
            session.rollback()
            print(f"Error saving to database: {e}")
        finally:
            session.close()
    else:
        quit()

main()




