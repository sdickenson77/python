This is a repo containing some example python scripts.  

greet.py - just a quick example of a prompt in python

graphy.py is an example graph using the matplot lib library

read_csv_example.py is a csv to sqlite database converted - it provides functionality to read CSV files and convert them
to SQLite database tables with appropriate column types.
  - films.csv is provided as example data - but any csv file can be used as long as correct location is specifcied.

test_csv_example.py - set of test cases to test the functions defined in read_csv_example.py  
  - to run tests both test file and regular file must be in same directory and can be run via cmd line using: pytest test_read_csv_example.py -v

