from utils import command_type

path_osmosis = "osmosis"
path_pbf = "input.pbf"
path_osm = "changeset.xml"
path_final = "output.pbf"


def apply_changes_pbf(path_osmosis, path_pbf, path_osm, path_final):
    return f'{path_osmosis} --read-pbf file={path_pbf} outPipe.0=pipe0 --read-xml-change file={path_osm} outPipe.0=pipe1 --apply-change inPipe.0=pipe0 inPipe.1=pipe1 outPipe.0=pipe2 --write-pbf file={path_final} inPipe.0=pipe2'

def update_db_xml(path_osmosis, path_osm):
    return f'{path_osmosis} --read-xml file={path_osm} --upload-apidb host="localhost" database="postgres" user="postgres" password="agro93ville" validateSchemaVersion=false'

def update_db_pbf(path_osmosis, path_pbf):
    return f'{path_osmosis} --read-pbf file={path_pbf} --upload-apidb host="localhost" database="postgres" user="postgres" password="agro93ville" validateSchemaVersion=false'

def osmosis_commands(command_type):
    command_dict = {
        command_type.CommandType.UPDATE_FILE: lambda: apply_changes_pbf(path_osmosis, path_pbf, path_osm, path_final),
        command_type.CommandType.UPDATE_DATABASE_XML: lambda: update_db_xml(path_osmosis, path_osm),
        command_type.CommandType.UPDATE_DATABASE_PBF: lambda: update_db_pbf(path_osmosis, path_pbf)
    }
    print("Valor de path_osm:", path_osm)
    command_function = command_dict.get(command_type)
    if command_function is None:
        print("Comando inválido.")
        return None
    
    command = command_function()
    print("Comando: ", command)
    return command

command_type = CommandType.UPDATE_DATABASE_PBF
osmosis_command = osmosis_commands(command_type)
