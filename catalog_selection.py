import json
import os
import subprocess
import sys


def generate_catalogs(streams_count, input_arr, branches, streams, PrimaryKeys, ReplicationMethod, ReplicationKeys, available_fields, automatic_fields, unsupported_fields, unselected_fields, available_fields_duplicate):

        folder_name = input_arr[branches]['folder_name']
        path = input_arr[branches]['path']
        tap_name = input_arr[branches]['tap_name']
        config_tap = input_arr[branches]['config_tap']
        python_version = input_arr[branches]['python_version']
        if os.system('cd '+path) != 0:
            sys.exit('path don\'t exist')

        config_json = json.loads(config_tap)
        with open(path+'/'+folder_name+'/tap-'+tap_name+'/config.json', "w") as s:
            json.dump(config_json, s)
        
        cmd_output = os.system('cd '+path+'/'+folder_name+'/tap-'+tap_name+'; python'+python_version+' -m venv venv; . venv/bin/activate; pip install -e .; tap-'+tap_name+' -c config.json -d > catalog.json')
        if cmd_output != 0:
            sys.exit()

        with open(path+'/'+folder_name+'/tap-'+tap_name+'/catalog.json', 'r') as f:
            json_load = json.load(f)

        data = json_load['streams']
        streams[branches] = []
        counts_stream = 0
        for x in range(int(len(data))):
            streams[branches].append(data[x]['stream'])
            counts_stream += 1
            for count in range(int(len(data[x]['metadata']))):
                if "forced-replication-method" in data[x]['metadata'][count]['metadata']:
                    ReplicationMethod[branches][data[x]['stream']] = data[x]['metadata'][count]['metadata']['forced-replication-method']
                if "valid-replication-keys" in data[x]['metadata'][count]['metadata']:
                    ReplicationKeys[branches][data[x]['stream']] = data[x]['metadata'][count]['metadata']['valid-replication-keys']
                    if type(ReplicationKeys[branches][data[x]['stream']]) != list:
                        ReplicationKeys[branches][data[x]['stream']] = [ReplicationKeys[branches][data[x]['stream']]]
                if "table-key-properties" in data[x]['metadata'][count]['metadata']:
                    PrimaryKeys[branches][data[x]['stream']] = data[x]['metadata'][count]['metadata']['table-key-properties']
                    if type(PrimaryKeys[branches][data[x]['stream']]) != list:
                        PrimaryKeys[branches][data[x]['stream']] = [PrimaryKeys[branches][data[x]['stream']]]
            if data[x]['stream'] not in ReplicationMethod[branches]:
                ReplicationMethod[branches][data[x]['stream']] = "-"
            if data[x]['stream'] not in ReplicationKeys[branches]:
                ReplicationKeys[branches][data[x]['stream']] = "-"
            unselected_fields[branches][data[x]['stream']] = "-"
            print(PrimaryKeys[branches][data[x]['stream']])
        streams_count[branches] = counts_stream

        for x in range(int(len(streams[branches]))):
            available_fields_list = []
            automatic_fields_list = []
            unsupported_fields_list = []
            with open(path+'/'+folder_name+'/tap-'+tap_name+'/catalog.json', 'r+') as f:
                json_load = json.load(f)
                data = json_load['streams']
                for count in range(int(len(data[x]['metadata']))):
                    if len(data[x]['metadata'][count]['breadcrumb']) == 0:
                        pass
                    elif data[x]['metadata'][count]['metadata']['inclusion'] == 'available':
                        if len(data[x]['metadata'][count]['breadcrumb']) == 2:
                            available_fields_list.append(data[x]['metadata'][count]['breadcrumb'][1])
                        if len(data[x]['metadata'][count]['breadcrumb']) > 2:
                            field_names = ''
                            for i in range(len(data[x]['metadata'][count]['breadcrumb'])):
                                if data[x]['metadata'][count]['breadcrumb'][i] != 'properties':
                                    field_names = field_names + '-&-' + data[x]['metadata'][count]['breadcrumb'][i]
                            available_fields_list.append(field_names)
                    elif data[x]['metadata'][count]['metadata']['inclusion'] == 'automatic':
                        if len(data[x]['metadata'][count]['breadcrumb']) == 2:
                            automatic_fields_list.append(data[x]['metadata'][count]['breadcrumb'][1])
                        if len(data[x]['metadata'][count]['breadcrumb']) > 2:
                            field_names = ''
                            for i in range(len(data[x]['metadata'][count]['breadcrumb'])):
                                if data[x]['metadata'][count]['breadcrumb'][i] != 'properties':
                                    field_names = field_names + '-&-' + data[x]['metadata'][count]['breadcrumb'][i]
                            automatic_fields_list.append(field_names)
                    elif data[x]['metadata'][count]['metadata']['inclusion'] == 'unsupported':
                        if len(data[x]['metadata'][count]['breadcrumb']) == 2:
                            unsupported_fields_list.append(data[x]['metadata'][count]['breadcrumb'][1])
                        if len(data[x]['metadata'][count]['breadcrumb']) > 2:
                            field_names = ''
                            for i in range(len(data[x]['metadata'][count]['breadcrumb'])):
                                if data[x]['metadata'][count]['breadcrumb'][i] != 'properties':
                                    field_names = field_names + '-&-' + data[x]['metadata'][count]['breadcrumb'][i]
                            unsupported_fields_list.append(field_names)
                if len(available_fields_list) == 0:
                    available_fields_list.append('-')
                available_fields[branches][data[x]['stream']] = available_fields_list
                available_fields_duplicate[branches][data[x]['stream']] = available_fields_list
                if len(automatic_fields_list) == 0:
                    automatic_fields_list.append('-')
                automatic_fields[branches][data[x]['stream']] = automatic_fields_list
                if len(unsupported_fields_list) == 0:
                    unsupported_fields_list.append('-')
                unsupported_fields[branches][data[x]['stream']] = unsupported_fields_list
            
        return streams, PrimaryKeys, ReplicationMethod, ReplicationKeys, available_fields, automatic_fields, unsupported_fields, unselected_fields, streams_count, available_fields_duplicate
    
def catalog_selection(main_file_path, input_arr, streams, available_fields, unselected_fields, different_stream, comparision, different_field, ReplicationMethod):
    
    key = list(streams.keys())[0]
            
    def select_stream(key, new_data, title, helper):

        cmd_output = os.system('rm -rf '+main_file_path+'/stream_select.sh')
        if cmd_output != 0:
            sys.exit()
        file_write = open(main_file_path+'/stream_select.sh', 'w')
        file_write.write('SELECT=$(whiptail --title "'+title+'" --checklist \ "'+helper+'" 20 90 15replace 3>&1 1>&2 2>&3)?\necho $SELECT')
        file_write.close()

        file_stream = open(main_file_path+'/stream_select.sh', 'r')
        filedata = file_stream.read()
        file_stream.close()

        newdata = filedata.replace("replace",new_data)

        file_stream = open(main_file_path+'/stream_select.sh', 'w')
        file_stream.write(newdata)
        file_stream.close()
        
        streams_select = subprocess.Popen(['sh', main_file_path+'/stream_select.sh'], stdout=subprocess.PIPE)
        stdout = streams_select.communicate()[0]
        stdout = stdout.decode("utf-8") 

        file_stream = open(main_file_path+'/stream_select.sh', 'r')
        filedata = file_stream.read()
        file_stream.close()
        newdata = filedata.replace(new_data, "replace")
        file_stream = open(main_file_path+'/stream_select.sh', 'w')
        file_stream.write(newdata)
        file_stream.close()

        return stdout
    
    cmd_output = os.system('rm -rf '+main_file_path+'/select.sh')
    if cmd_output != 0:
        sys.exit()
    file_write = open(main_file_path+'/select.sh', 'w')
    file_write.write('SELECT=$(whiptail --title "Field Selection" --yesno "Need to select specific stream?" 8 78 3>&1 1>&2 2>&3)\nexitstatus=$?\necho -n "$exitstatus"')
    file_write.close()
    streams_select = subprocess.Popen(['sh', main_file_path+'/select.sh'], stdout=subprocess.PIPE)
    stdout = streams_select.communicate()[0]
    stdout = stdout.decode("utf-8")
    if stdout == '0':
        new_data = ""
        print(ReplicationMethod)
        if comparision:
            for stream in list(set(streams[key]) - set(different_stream[key])):
                new_data = new_data + ' \ "' + stream + '" " '+ ReplicationMethod[key][stream] + '" OFF'
        else:
            for stream in streams[key]:
                new_data = new_data + ' \ "' + stream + '" " '+ ReplicationMethod[key][stream] + '" OFF'

        stdout = select_stream(key, new_data, input_arr[key]['tap_name'], "Select Streams")

        stdout = stdout.replace('" "', ',')
        stdout = stdout.replace('"', '')
        stdout = stdout.split(",")
        if stdout[0] != '' or stdout[0] != '?\n':
            selected_streams = []
            for stream in stdout:
                    stream = stream.replace(" ", "")
                    stream = stream.replace("?\n", "")
                    selected_streams.append(stream)
                    
            for branches in list(input_arr.keys()):
                    streams[branches] = list(set(streams[branches]) & set(selected_streams))
                    if comparision:
                        streams[branches] = list(set(streams[branches]).union(set(different_stream[branches])))

    streams_select = subprocess.Popen(['sh', main_file_path+'/select.sh'], stdout=subprocess.PIPE)
    stdout = streams_select.communicate()[0]
    stdout = stdout.decode("utf-8")
    if stdout == '0':
                if comparision:
                    for stream in list(set(streams[key]) - set(different_stream[key])):
                        new_data = ""
                        for field in list(set(available_fields[key][stream]) - set(different_field[key][stream])):
                            new_data = new_data + ' \ "' + field + '" "" ON'

                        stdout = select_stream(key, new_data, stream, "Select Fields")
                        stdout = stdout.replace('" "', ',')
                        stdout = stdout.replace('"', '')
                        stdout = stdout.split(",")
                        selected_fields = []
                        if stdout[0] != '' or stdout[0] != '?\n':
                            for field in stdout:
                                            field = field.replace(" ", "")
                                            field = field.replace("?\n", "")
                                            if field != "":
                                                selected_fields.append(field)
                            for branches in list(input_arr.keys()):
                                            unselected_fields[branches][stream] = list(set(available_fields[branches][stream]) - set(selected_fields)) 
                                            available_fields[branches][stream] = list((set(available_fields[branches][stream]) & set(selected_fields)).union(different_field[branches][stream]))
                else:
                    for stream in streams[key]:
                        new_data = ""
                        for field in available_fields[key][stream]:
                            new_data = new_data + ' \ "' + field + '" "" ON'

                        stdout = select_stream(key, new_data, stream, "Select Fields")
                        stdout = stdout.replace('" "', ',')
                        stdout = stdout.replace('"', '')
                        stdout = stdout.split(",")
                        selected_fields = []
                        for field in stdout:
                                        field = field.replace(" ", "")
                                        field = field.replace("?\n", "")
                                        if field != "":
                                            selected_fields.append(field)
                        for branches in list(input_arr.keys()):
                                        unselected_fields[branches][stream] = list(set(available_fields[branches][stream]) - set(selected_fields)) 
                                        available_fields[branches][stream] = list(set(available_fields[branches][stream]) & set(selected_fields))
    
    return streams, unselected_fields, available_fields

def catalog_update(input_arr, streams, available_fields, unselected_fields, streams_count):
    
    for branches in input_arr:

        print("\n@@@\nUpdating catalog of branch > {}".format(branches))
        print("-----------------------------------------")
        folder_name = input_arr[branches]['folder_name']
        path = input_arr[branches]['path']
        tap_name = input_arr[branches]['tap_name']
        for x in range(streams_count[branches]):
            with open(path+'/'+folder_name+'/tap-'+tap_name+'/catalog.json', 'r+') as f:
                    json_load = json.load(f)
                    data = json_load['streams']
                    if data[x]['stream'] in streams[branches]:
                        print("Selecting fields of Stream > {}".format(data[x]['stream']))
                        for count in range(int(len(data[x]['metadata']))):
                            if len(data[x]['metadata'][count]['breadcrumb']) == 0:
                                data[x]['metadata'][count]['metadata'].update({"selected": True})
                                f.seek(0)
                                json.dump(json_load, f, indent=5)
                            elif data[x]['metadata'][count]['breadcrumb'][1] in available_fields[branches][data[x]['stream']]:
                                data[x]['metadata'][count]['metadata'].update({"selected": True})
                                f.seek(0)
                                json.dump(json_load, f, indent=5)
                            elif data[x]['metadata'][count]['breadcrumb'][1] in unselected_fields[branches][data[x]['stream']]:
                                data[x]['metadata'][count]['metadata'].update({"selected": False})
                                f.seek(0)
                                json.dump(json_load, f, indent=5)
                    f.close()
