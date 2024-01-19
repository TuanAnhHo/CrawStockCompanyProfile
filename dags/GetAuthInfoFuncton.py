from configparser import ConfigParser
import os

## Function to parse Authorization from .ini files
## Parameters: 
#   filename: path of .ini file 
#   section: header of config you want to get which is displayed in square bracket /[]/

def ReadConfigFile(filename:str, section:str):
    
    try:
        text_file_path = os.path.abspath(filename)
    except:
        raise "Failed to get path of config file."
    
    parser = ConfigParser()
    parser.read(text_file_path)    
    config_info = {}
    
    if parser.has_section(section=section):
        params = parser.items(section=section)
        for param in params:
            config_info[param[0]] = param[1]
    else:
        raise Exception('Section {section} not found in the {text_file_path} file'.format(section=section, text_file_path=text_file_path))        
    
    return config_info



