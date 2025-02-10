import re
import os
import pandas as pd
from datetime import datetime

# import the dealt with folder to a duckdb database 

def import_data():
    folder_path = '../Dealt With/'
    files = os.listdir(folder_path)
    files.to_csv('reviewer.csv',index = False)




def main():
    import_data()