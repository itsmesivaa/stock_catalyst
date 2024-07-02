import configparser

def load_config(config_file_path):
    # Read config file
    config = configparser.ConfigParser()
    config.read(config_file_path)

    # Get database connection parameters
    username = config['database']['username']
    password = config['database']['password']
    host = config['database']['host']
    database = config['database']['database']
    driver = config['database']['driver']

    # Get database connection parameters
    #username = config['aws_database']['username']
    #password = config['aws_database']['password']
    #host = config['aws_database']['host']
    #database = config['aws_database']['database']
    #driver = config['aws_database']['driver']
    
    return username,password,host,database,driver
