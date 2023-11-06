from utils import command_type, files_utils

path_osmosis = "osmosis"
path_pbf = "input.pbf"
path_osm = "changeset.xml"
path_final = "output.pbf"
type = command_type.CommandType

def apply_changes_pbf(path_osmosis, path_pbf, path_osm, path_final):
    return f'{path_osmosis} --read-pbf file={path_pbf} outPipe.0=pipe0 --read-xml-change file={path_osm} outPipe.0=pipe1 --apply-change inPipe.0=pipe0 inPipe.1=pipe1 outPipe.0=pipe2 --write-pbf file={path_final} inPipe.0=pipe2'

def update_db_xml(path_osmosis, path_osm):
    return f'{path_osmosis} --read-xml file={path_osm} --upload-apidb host="localhost" database="postgres" user="postgres" password="agro93ville" validateSchemaVersion=false'

def update_db_pbf(path_osmosis, path_pbf):
    return f'{path_osmosis} --read-pbf file={path_pbf} --upload-apidb host="localhost" database="postgres" user="postgres" password="agro93ville" validateSchemaVersion=false'

# def merge_osc(path_osmosis, f_osc, s_osc,)
def merge_osc_files(download_dir, file_paths, number_of_files):

    changes_file = download_dir + "changes.osc"

    if number_of_files == 1:
        osc_file = file_paths[0]
        os.rename(osc_file, changes_file)
        return changes_file
    
    merged_file_path = download_dir + "tmp.osc"

    merged_final_file = merged_file_path.format(str(1))

    merge_command = get_merge_command(file_paths[0], file_paths[1], merged_final_file)
    
    result = subprocess.run(merge_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if result.returncode == 0:
        print("Comando executado com sucesso")
        print("Saída padrão:", result.stdout)
    else:
        print("O comando falhou")
        print("Erro padrão:", result.stderr)

    os.remove(file_paths[0])
    os.remove(file_paths[1])

    if number_of_files == 2:
        os.rename(merged_final_file, changes_file)
        return changes_file
    else:
        for i in range(2, number_of_files):
            old_merged_file = merged_final_file
            new_merged_file = merged_file_path.format(str(i))
            osc_file = file_paths[i]
            merge_command = get_merge_command(old_merged_file, osc_file, new_merged_file)
            subprocess.run(merge_command)
            os.remove(osc_file)
            os.remove(old_merged_file)
            merged_final_file = new_merged_file

        os.rename(merged_final_file, changes_file)
        return changes_file
        
    
def get_merge_command(read_file_one, read_file_two, write_file):
    return [
    OSMOSIS_BASE_PATH,
    '--read-xml-change', f'file={read_file_one}',
    '--read-xml-change', f'file={read_file_two}',
    '--merge-change',
    '--write-xml-change', f'file={write_file}'
    ]

def osmosis_commands(command_type):
    command_dict = {
        type.UPDATE_FILE: lambda: apply_changes_pbf(path_osmosis, path_pbf, path_osm, path_final),
        type.UPDATE_DATABASE_XML: lambda: update_db_xml(path_osmosis, path_osm),
        type.UPDATE_DATABASE_PBF: lambda: update_db_pbf(path_osmosis, path_pbf)
    }
    print("Valor de path_osm:", path_osm)
    command_function = command_dict.get(command_type)
    if command_function is None:
        print("Comando inválido.")
        return None
    
    command = command_function()
    print("Comando: ", command)
    return command



def sh_merge_osc(download_dir):
    return f'''#!/bin/bash

    python3 - <<END
        import os
        import subprocess

        OSMOSIS_BASE_PATH = "/osmosis/package/bin/osmosis "

        def merge_update_files(download_dir):
            if download_dir is not None:
                file_paths = files_utils.get_osc_file_paths(download_dir)
                number_of_files = len(file_paths)
                if number_of_files > 0:
                    file_path = merge_osc_files(download_dir, file_paths, number_of_files)
                    return file_path
                else:
                    return None
            else:
                return None

        def merge_osc_files(download_dir, file_paths, number_of_files):

            changes_file = download_dir + "changes.osc"

            if number_of_files == 1:
                osc_file = file_paths[0]
                os.rename(osc_file, changes_file)
                return changes_file
            
            merged_file_path = download_dir + "tmp.osc"

            merged_final_file = merged_file_path.format(str(1))

            merge_command = get_merge_command(file_paths[0], file_paths[1], merged_final_file)
            
            result = subprocess.run(merge_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            if result.returncode == 0:
                print("Comando executado com sucesso")
                print("Saída padrão:", result.stdout)
            else:
                print("O comando falhou")
                print("Erro padrão:", result.stderr)

            os.remove(file_paths[0])
            os.remove(file_paths[1])

            if number_of_files == 2:
                os.rename(merged_final_file, changes_file)
                return changes_file
            else:
                for i in range(2, number_of_files):
                    old_merged_file = merged_final_file
                    new_merged_file = merged_file_path.format(str(i))
                    osc_file = file_paths[i]
                    merge_command = get_merge_command(old_merged_file, osc_file, new_merged_file)
                    subprocess.run(merge_command)
                    os.remove(osc_file)
                    os.remove(old_merged_file)
                    merged_final_file = new_merged_file

                os.rename(merged_final_file, changes_file)
                return changes_file
            
        def get_merge_command(read_file_one, read_file_two, write_file):
            return [
            OSMOSIS_BASE_PATH,
            '--read-xml-change', 'file=' + read_file_one,
            '--read-xml-change', 'file=' + read_file_two,
            '--merge-change',
            '--write-xml-change', 'file=' + write_file
            ]

        merge_update_files("{download_dir}")
        END

    '''

def get_python_code(download_dir):
    if(download_dir is not None): file_paths = files_utils.get_osc_file_paths(download_dir)
    return f'''
    if {download_dir} is not None:
        number_of_files = len({file_paths})
        if number_of_files > 0:
              changes_file = {download_dir} + "changes.osc"
                if number_of_files == 1:
                    osc_file = file_paths[0]
                    os.rename(osc_file, changes_file)
                    return changes_file
                
                merged_file_path = download_dir + "tmp.osc"

                merged_final_file = merged_file_path.format(str(1))

                merge_command = [{OSMOSIS_BASE_PATH}, --read-xml-change] 
                {get_merge_command(file_paths[0], file_paths[1], merged_final_file)}

                def get_merge_command(read_file_one, read_file_two, write_file):
                        return [
                        ,
                        '--read-xml-change', f'file={read_file_one}',
                        '--read-xml-change', f'file={read_file_two}',
                        '--merge-change',
                        '--write-xml-change', f'file={write_file}'
                        ]
                
                result = subprocess.run(merge_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                if result.returncode == 0:
                    print("Comando executado com sucesso")
                    print("Saída padrão:", result.stdout)
                else:
                    print("O comando falhou")
                    print("Erro padrão:", result.stderr)

                os.remove(file_paths[0])
                os.remove(file_paths[1])

                if number_of_files == 2:
                    os.rename(merged_final_file, changes_file)
                    return changes_file
                else:
                    for i in range(2, number_of_files):
                        old_merged_file = merged_final_file
                        new_merged_file = merged_file_path.format(str(i))
                        osc_file = file_paths[i]
                        merge_command = get_merge_command(old_merged_file, osc_file, new_merged_file)
                        subprocess.run(merge_command)
                        os.remove(osc_file)
                        os.remove(old_merged_file)
                        merged_final_file = new_merged_file

                    os.rename(merged_final_file, changes_file)
                    file_path = changes_file
            return file_path
        else:
            return None
    else:
        return None
    '''

