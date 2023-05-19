from collections import defaultdict
from platform import python_version
from catalog_selection import *
from comparision import *
from sync import *
import datetime

try:
    import ConfigParser as cp
except:
    import configparser as cp
    
def user_inputs(main_file_path, input_arr):
    
    try:
        config = cp.ConfigParser(allow_no_value=True)
        config.read(os.path.join(main_file_path,"inputs.conf"))
        for branches in config.sections():
                file_already_present = config.get(branches, "file_already_present")
                if file_already_present == "":
                    raise Exception("\n-- Please provide yes/no in 'file_already_present' --\n")
                property = config.get(branches, "property")
                if property == "":
                    raise Exception("\n-- Please provide --catalog/-p in 'property' --\n")
                tap_name = config.get(branches, "tap_name")
                if tap_name == "":
                    raise Exception("\n-- Please provide Tap name to be worked on --\n")
                path = config.get(branches, "path")
                if path == "":
                    raise Exception("\n-- Please provide path to create/consume the Tap --\n")
                folder_name = config.get(branches, "folder_name")
                if folder_name == "":
                    raise Exception("\n-- Please provide folder name --\n")
                config_tap =  config.get(branches, "config_tap")
                if config_tap == "":
                    raise Exception("\n-- Please provide config --\n")
                python_version = config.get(branches, "python_version")
                if python_version == "":
                    raise Exception("\n-- Please provide python_version --\n")
                if config.get(branches, "bookmark_tap"):
                    bookmark_tap = config.get(branches, "bookmark_tap")                       
                    if bookmark_tap != "" and ("bookmarks" in list(json.loads(bookmark_tap).keys())):
                        print("\n-- sync with bookmark will run with: --\n-- Bookmark: {}".format(bookmark_tap))
                        input_arr[branches]['bookmark_tap'] = bookmark_tap                       
                input_arr[branches]['file_already_present'] = file_already_present
                input_arr[branches]['property'] = property
                input_arr[branches]['tap_name'] = tap_name
                input_arr[branches]['path'] = path
                input_arr[branches]['folder_name'] = folder_name
                input_arr[branches]['config_tap'] = config_tap
                input_arr[branches]['python_version'] = python_version
                
        return input_arr

    except Exception as err:
        print(err)
        exit()

def clone_tap(input_arr, branches):
    
    folder_name = input_arr[branches]['folder_name']
    path = input_arr[branches]['path']
    tap_name = input_arr[branches]['tap_name']
    branch = branches
    python_version = input_arr[branches]['python_version']
    if os.system('cd '+path+'/'+folder_name) == 0:
        path_update = input('folder_name+" in the "+path+" already exist.\n(1) To owerite the Folder Enter.\n(2) To exit.\nEnter your choice here: ')
        if path_update == '1':
            cmd_output = os.system('cd '+path+'; rm -rf '+folder_name)
            if cmd_output != 0:
                sys.exit()
        elif path_update == '2':
            sys.exit('Stopping utility here.')
        else:
            sys.exit('Input was not specified. Stopping utility here.')
    
    if os.system('cd '+path) == 0:
        cmd_output = os.system('cd '+path+'; mkdir '+folder_name+'; cd '+folder_name+'; git clone https://github.com/singer-io/tap-'+tap_name+'.git; cd tap-'+tap_name+'; git checkout '+branch+'; python'+python_version+' -m venv venv; . venv/bin/activate; pip install -e .')
        if cmd_output != 0:
            sys.exit()
    else:
        cmd_output = os.system('mkdir '+path+'; cd '+path+'; mkdir '+folder_name+'; cd '+folder_name+'; git clone https://github.com/singer-io/tap-'+tap_name+'.git; cd tap-'+tap_name+'; git checkout '+branch+'; python'+python_version+' -m venv venv; . venv/bin/activate; pip install -e .')
        if cmd_output != 0:
            sys.exit()

def main():
    
    os.system('echo "    starting Talend Utilty   " | boxes -d parchment')
    main_file_path = os.getcwd()
    input_arr = defaultdict(dict)
    time_start = datetime.datetime.now()
    time_start = str(time_start.strftime("%m-%d-%Y_%H:%M:%S"))

    user_inputs(main_file_path, input_arr)
    branch_names = []
    name_tap = []
    for branches in input_arr:
        branch_names.append(branches)
        name_tap.append(input_arr[branches]["tap_name"])

    name_tap = set(name_tap)
    for n_t in name_tap:
        if os.system('cd ' + main_file_path + '/results_' + n_t) != 0:
            cmd_output = os.system('cd ' + main_file_path + '; mkdir results_'+name_tap)
            if cmd_output != 0:
                sys.exit("Folder was not able to create...")
        cmd_output = os.system('cd ' + main_file_path + '/results_' + n_t + '; mkdir ' + time_start)
        if cmd_output != 0:
            sys.exit("Folder was not able to create...")
    
    do_sync = True
    comparision = False
    if len(input_arr) == 2:
        if input_arr[branch_names[0]]['tap_name'] == input_arr[branch_names[1]]['tap_name']:
            comparision = True
            do_sync = False
            cmd_output = os.system('rm -rf select.sh')
            if cmd_output != 0:
                sys.exit()
            file_write = open(main_file_path+'/select.sh', 'w')
            file_write.write('SELECT=$(whiptail --title "Selection" --radiolist \"Choose user\'s permissions" 10 90 4 \"Without sync" "" ON \"With Sync" "" OFF 3>&1 1>&2 2>&3)\necho -n "$SELECT"')
            file_write.close()
            streams_select = subprocess.Popen(['sh', main_file_path+'/select.sh'], stdout=subprocess.PIPE)
            stdout = streams_select.communicate()[0]
            stdout = stdout.decode("utf-8")
            print("\n--------------",stdout,"--------------\n")
            if "With Sync" in stdout:
                do_sync = True
    
    streams = defaultdict(dict)
    streams_count = defaultdict(dict)
    PrimaryKeys = defaultdict(dict)
    ReplicationMethod = defaultdict(dict)
    ReplicationKeys = defaultdict(dict)
    values_replication = defaultdict(dict)

    available_fields = defaultdict(dict)
    available_fields_duplicate = defaultdict(dict)
    unselected_fields = defaultdict(dict)
    automatic_fields = defaultdict(dict)
    unsupported_fields = defaultdict(dict)

    newly_added_fields = defaultdict(dict)
    removed_fields = defaultdict(dict)
    added_uncommon_pairs = defaultdict(dict)
    removed_uncommon_pairs = defaultdict(dict)
    updated_uncommon_pairs = defaultdict(dict)
    final_fields = defaultdict(dict)
    final_fields_format = defaultdict(dict)
    added_format = defaultdict(dict)
    removed_format = defaultdict(dict)

    for branches in input_arr:
        
        if input_arr[branches]['file_already_present'].lower() == 'no':       
            clone_tap(input_arr, branches)

        print("\n---------------------Generating catalog of {} branch------------\n".format(branches))
        generate_catalogs(streams_count, input_arr, branches, streams, PrimaryKeys, ReplicationMethod, ReplicationKeys, available_fields, automatic_fields, unsupported_fields, unselected_fields, available_fields_duplicate)
        print("---------------------Catalog generated------------")

    different_stream = {}
    different_field = defaultdict(dict)
    if comparision:
        different_stream[branch_names[0]] = list(set(streams[branch_names[0]]) - set(streams[branch_names[1]]))
        different_stream[branch_names[1]] = list(set(streams[branch_names[1]]) - set(streams[branch_names[0]]))
        for stream in list(set(streams[branch_names[0]]) - set(different_stream[branch_names[0]])):
            different_field[branch_names[0]][stream] = list(set(available_fields[branch_names[0]][stream]) - set(available_fields[branch_names[1]][stream]))
        for stream in list(set(streams[branch_names[1]]) - set(different_stream[branch_names[1]])):
            different_field[branch_names[1]][stream] = list(set(available_fields[branch_names[1]][stream]) - set(available_fields[branch_names[0]][stream]))

        os.system('echo "Comapring catalog\'s..." | boxes -d stone')
        tap_name = input_arr[branch_names[0]]['tap_name']
        catalog_compare(input_arr, newly_added_fields, removed_fields, available_fields, automatic_fields, unsupported_fields, streams, added_uncommon_pairs, removed_uncommon_pairs, final_fields, final_fields_format, added_format, removed_format)
        os.system('echo "Generating comparision report..." | boxes -d ada-box')
        genrate_comparision_report(input_arr, main_file_path, streams, added_uncommon_pairs, removed_uncommon_pairs, newly_added_fields, removed_fields, updated_uncommon_pairs, ReplicationMethod, ReplicationKeys, automatic_fields, unsupported_fields, available_fields, added_format, removed_format)
        cmd_output = os.system('mv -f '+tap_name+'_Comparision.html results_'+tap_name+'/'+ time_start+'/')
        if cmd_output != 0:
            sys.exit()

    if do_sync:

        catalog_selection(main_file_path, input_arr, streams, available_fields, unselected_fields, different_stream, comparision, different_field, ReplicationMethod)
        catalog_update(input_arr, streams, available_fields, unselected_fields, streams_count)
        
        sync1 = defaultdict(dict)
        unique_sync1 = defaultdict(dict)
        sync2 = defaultdict(dict)
        syncs = defaultdict(dict)
        bookmarks = defaultdict(dict)
        bookmarks_updated = defaultdict(dict)
        max_bookmark_value = defaultdict(dict)
        max_bookmark_value_state = defaultdict(dict)

        os.system('echo "Going for sync...." | boxes -d ada-box')
        for branches in input_arr:
            print("---------------------Starting sync of {} branch---------------------".format(branches))
            sync(streams, input_arr, PrimaryKeys, branches, sync1, unique_sync1, sync2, syncs, bookmarks, bookmarks_updated, values_replication, ReplicationKeys, ReplicationMethod, max_bookmark_value, max_bookmark_value_state)
            print("---------------------Generating sync report of {} branch------------".format(branches))
            genrate_sync_report(input_arr, main_file_path, branches, unselected_fields, unsupported_fields, automatic_fields, ReplicationKeys, ReplicationMethod, PrimaryKeys, streams, available_fields, sync1, unique_sync1, syncs, bookmarks, bookmarks_updated, max_bookmark_value, max_bookmark_value_state)
            os.system('echo "--"; echo "sync report generated" | boxes -d ada-cmt; echo "--"')
            tap_name = input_arr[branches]['tap_name']
            folder_name = input_arr[branches]['folder_name']
            cmd_output = os.system("mv -f "+tap_name+"_"+folder_name+"_"+branches+".html results_"+tap_name+"/"+time_start+'/')
            if cmd_output != 0:
                sys.exit()

        print("\nUtility terminated...\n")

if __name__ == "__main__":
    main()
