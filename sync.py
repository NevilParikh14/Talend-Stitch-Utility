import itertools
import json
import os
import re
import sys
import time
from more_itertools import ilen


def sync(streams, input_arr, PrimaryKeys, branches, sync1, unique_sync1, sync2, syncs, bookmarks, bookmarks_updated, values_replication, ReplicationKeys, ReplicationMethod, max_bookmark_value, max_bookmark_value_state):

    folder_name = input_arr[branches]['folder_name']
    property = input_arr[branches]['property']
    path = input_arr[branches]['path']
    tap_name = input_arr[branches]['tap_name']
    state_value = ''
    cmd_output = os.system('cd '+path+'/'+folder_name+'/tap-'+tap_name+'; . venv/bin/activate; tap-'+tap_name+' -c config.json '+property+' catalog.json > sync1.json')
    if cmd_output != 0:
            sys.exit()

    def max_rep_val(lines, stream, rep_key):
        for line in lines:
            if re.findall(r'"type": "RECORD"', line) and re.findall(r'"stream": "'+stream+r'"', line):
                json_load = json.loads(line)
                values = json_load['record']
                if set(rep_key).issubset(set(list(values.keys()))) and type(values[rep_key[0]]) == str:
                    yield values[rep_key[0]]
                else:
                    yield '-'
            else:
                yield '-'

    def events_gen(lines, stream):
        for line in lines:
            if re.findall(r'"type": "RECORD"', line) and re.findall(r'"stream": "'+stream+r'"', line):
                json_load = json.loads(line)
                values = json_load['record']
                yield values
                             
    for stream in streams[branches]:
        with open(path+'/'+folder_name+'/tap-'+tap_name+'/sync1.json') as s:    
            stream = str(stream)
            lines = s.read().split('\n')
            is_incre = False
            sync1[branches][stream] = 0
            if lines:
                events = events_gen(lines, stream)
                events, counts = itertools.tee(events)
                events, events_1 = itertools.tee(events)
                sync1[branches][stream] = ilen(counts)
                if ReplicationMethod[branches][stream] == 'INCREMENTAL':
                    print(stream)
                    rep_key = ReplicationKeys[branches][stream]
                    max_bookmark_val = max_rep_val(lines, stream, rep_key)
                    max_bookmark_value[branches][stream] = max(max_bookmark_val)
                else:
                    max_bookmark_value[branches][stream] = '-'
            else:
                max_bookmark_value[branches][stream] = '-'

        if stream in PrimaryKeys[branches] and PrimaryKeys[branches][stream] != '-':
            print(PrimaryKeys[branches][stream])
            print(type(PrimaryKeys[branches][stream]))
            tuples_new_pk = (tuple([e[k] for k in PrimaryKeys[branches][stream]]) for e in events)
            set_counts = int(len(set(tuples_new_pk)))
            tuples_new_pk = (tuple([e[k] for k in PrimaryKeys[branches][stream]]) for e in events_1)
            list_counts = int(ilen(tuples_new_pk))
            unique_sync1[branches][stream] = [list_counts, set_counts]

    def find_bookmark(s, line_no, lines):
        
        last_line = lines[line_no]
        
        json_load = json.loads(last_line)
        return json_load

    if "bookmark_tap" in list(input_arr[branches].keys()):
        state_value = json.loads(input_arr[branches]["bookmark_tap"])
        if len(state_value) != 0:
            for y in list(state_value['bookmarks'].keys()):
                bookmarks[branches][y] = state_value['bookmarks'][y]
            for y in streams[branches]:
                if y not in list(bookmarks[branches].keys()):
                    bookmarks[branches][y] = list('-')
        else:
            for y in streams[branches]:
                bookmarks[branches][y] = list('-')
    else: 
        with open(path+'/'+folder_name+'/tap-'+tap_name+'/sync1.json') as s:            
            line_no = -1
            lines = s.read().splitlines()
            if len(lines) != 0:
                json_load = find_bookmark(s, line_no, lines)
                print(json_load)
                if json_load['type'] == "STATE":
                    if "value" in list(json_load.keys()):
                        state_value = json_load['value']
                else:
                    sub = 1
                    line_len = 2
                    while "value" not in list(json_load.keys()):
                        if line_len < len(lines):
                            line_no = -1-sub
                            json_load = find_bookmark(s, line_no, lines)
                            sub += 1
                            line_len +=1
                        else:
                            break
                    if json_load['type'] == "STATE":
                        if "value" in list(json_load.keys()):
                            state_value = json_load['value']
                    else:
                        state_value = ''
                print(len(state_value))
                if len(state_value) != 0 and "bookmarks" in list(state_value.keys()):
                    print("----------")
                    for y in list(state_value['bookmarks'].keys()):
                        print(state_value['bookmarks'].keys())
                        bookmarks[branches][y] = state_value['bookmarks'][y]
                    for y in streams[branches]:
                        if y not in list(bookmarks[branches].keys()):
                            bookmarks[branches][y] = list('-')
                else:
                    for y in streams[branches]:
                        bookmarks[branches][y] = list('-')
            else:
                for y in streams[branches]:
                    bookmarks[branches][y] = list('-')

    cmd_output = os.system('rm -rf '+path+'/'+folder_name+'/tap-'+tap_name+'/state.json')
    if cmd_output != 0:
            sys.exit()
    with open(path+'/'+folder_name+'/tap-'+tap_name+'/state.json', "w") as s:
        json.dump(state_value, s)
 
    cmd_output = os.system('cd '+path+'/'+folder_name+'; cd tap-'+tap_name+'; . venv/bin/activate; tap-'+tap_name+' -c config.json '+property+' catalog.json -s state.json > syncs.json')
    if cmd_output != 0:
        sys.exit()
    
    for stream in streams[branches]:
        if ReplicationMethod[branches][stream] == 'INCREMENTAL':
            with open(path+'/'+folder_name+'/tap-'+tap_name+'/syncs.json') as s:    
                stream = str(stream)
                lines = s.read().split('\n')
                if lines:
                    rep_key = ReplicationKeys[branches][stream]
                    max_bookmark_val = max_rep_val(lines, stream, rep_key)
                    max_bookmark_value_state[branches][stream] = max(max_bookmark_val)
                else:
                    max_bookmark_value_state[branches][stream] = '-'
        else:
            max_bookmark_value_state[branches][stream] = '-'
    
    for stream in streams[branches]:
            with open(path+'/'+folder_name+'/tap-'+tap_name+'/syncs.json') as s:    
                stream = str(stream)
                counts = 0
                for line in s.read().split('\n'):
                    if re.findall(r'"type": "RECORD"', line) and re.findall(r'"stream": "'+stream+r'"', line):
                        counts += 1
                syncs[branches][stream] = counts
    
    with open(path+'/'+folder_name+'/tap-'+tap_name+'/syncs.json') as s:
        
        line_no = -1
        lines = s.read().splitlines()
        if len(lines) != 0:
            json_load = find_bookmark(s, line_no, lines)
            print(json_load)
            if json_load['type'] == "STATE":
                if "value" in list(json_load.keys()):
                    state_value = json_load['value']
            else:
                sub = 1
                line_len = 2
                while "value" not in list(json_load.keys()):
                    if line_len < len(lines):
                        line_no = -1-sub
                        json_load = find_bookmark(s, line_no, lines)
                        sub += 1
                        line_len +=1
                    else:
                        break
                if json_load['type'] == "STATE":
                    if "value" in list(json_load.keys()):
                        state_value = json_load['value']
                else:
                    state_value = ''
                print(len(state_value))
            if len(state_value) != 0 and "bookmarks" in list(state_value.keys()):
                for y in list(state_value['bookmarks'].keys()):
                    bookmarks_updated[branches][y] = state_value['bookmarks'][y]
                for y in streams[branches]:
                    if y not in list(bookmarks_updated[branches].keys()):
                        bookmarks_updated[branches][y] = list('-')
            else:
                for y in streams[branches]:
                    bookmarks_updated[branches][y] = list('-')
        else:
            for y in streams[branches]:
                bookmarks_updated[branches][y] = list('-')

    return sync1, unique_sync1, sync2, syncs, bookmarks, bookmarks_updated, values_replication, max_bookmark_value, max_bookmark_value_state


def genrate_sync_report(input_arr, main_file_path, branches, unselected_fields, unsupported_fields, automatic_fields, ReplicationKeys, ReplicationMethod, PrimaryKeys, streams, available_fields, sync1, unique_sync1, syncs, bookmarks, bookmarks_updated, max_bookmark_value, max_bookmark_value_state):

        top = '''
        <!DOCTYPE html>
        <html>
        <style>
        table, th, td {
        border:1px solid black;
        vertical-align: top;
        text-align: center;
        }
        tr:nth-child(even) {
        background-color: #dddddd;
        }
        ul {
        display: inline-block;
        text-align: left;
        vertical-align: top;
        }
        </style>
        <body>
        '''

        end_text = '''
        </body>
        </html>
        '''
        
        folder_name = input_arr[branches]['folder_name']
        path = input_arr[branches]['path']
        tap_name = input_arr[branches]['tap_name']

        cmd_output = os.system('rm -rf '+main_file_path+'/'+tap_name+'_'+folder_name+'_'+branches+'.html')
        if cmd_output != 0:
            sys.exit()
        file = open(main_file_path+"/"+tap_name+"_"+folder_name+"_"+branches+".html","a")
        file.write(top)
        
        body = '''
        <table style="width:100%; text-align: center"><td style='border:3px solid black; border-style: dashed;'><h1>Tap-{name}</h1>
        <h3>Branch: {branch}</h3>
        <h3>Path: {path}</h3>
        </td></table>
        '''.format(name=tap_name, branch=branches, path=path+'/'+folder_name)

        file.write(body)
        file.write("<table style='width:100%;'><tr><th>Stream</th><th>Primary-Key</th><th>Repication Method</th><th>Replication-Key</th><th>Available-Fields</th><th>Automatic-Fields</th><th>Unsupported Fields</th></tr>")
        
        for y in streams[branches]:
        
            file.write("<tr><td><p style='text-align: center;'><b>{stream}</b></b></td><td>".format(stream=y))
            # PrimaryKeys[branches][y].sort()
            file.write("<ul>")
            for field in PrimaryKeys[branches][y]: 
                file.write("<li>{added}</li>".format(added=field))
            file.write("</ul>")
            file.write("</td><td><p style='text-align: center;'>{replication_method}</p></td><td>".format(replication_method=ReplicationMethod[branches][y]))
            
            file.write("<ul>")
            for field in ReplicationKeys[branches][y]: 
                file.write("<li>{added}</li>".format(added=field))
            file.write("</ul>")
            file.write("</td><td>")
            
            available_fields[branches][y].sort()
            count_temp = 0
            count_max = len(available_fields[branches][y])
            file.write("<table style='width:100%;'><tr><td>")
            file.write("<p style='text-align: center;'><b>selected</b></p>")
            if count_max > 5:
                for field in available_fields[branches][y]: 
                    if count_temp % (int(count_max/2)+1) == 0:
                        file.write("<ul>")
                    file.write("<li>{added}</li>".format(added=field))
                    if (count_temp+1) % (int(count_max/2)+1) == 0 or (count_temp+1) == count_max:
                        file.write("</ul>")
                    count_temp += 1
            else:
                file.write("<ul>")
                for field in available_fields[branches][y]:
                    file.write("<li>{added}</li>".format(added=field))
                file.write("</ul>")
            file.write("</td><td>")
            
            count_temp = 0
            count_max = len(unselected_fields[branches][y])
            file.write("<p style='text-align: center;'><b>Unselected</b></p>")
            if count_max > 5:
                for field in unselected_fields[branches][y]: 
                    if count_temp % (int(count_max/2)+1) == 0:
                        file.write("<ul>")
                    file.write("<li>{added}</li>".format(added=field))
                    if (count_temp+1) % (int(count_max/2)+1) == 0 or (count_temp+1) == count_max:
                        file.write("</ul>")
                    count_temp += 1
            else:
                file.write("<ul>")
                for field in unselected_fields[branches][y]:
                    file.write("<li>{added}</li>".format(added=field))
                file.write("</ul>")
            file.write("</td></tr></table></td><td>")

            file.write("<ul>")
            for field in automatic_fields[branches][y]: 
                file.write("<li>{added}</li>".format(added=field))
            file.write("</ul>")
            file.write("</td><td>")

            file.write("<ul>")
            for field in unsupported_fields[branches][y]: 
                file.write("<li>{added}</li>".format(added=field))
            file.write("</ul>")
            file.write("</td></tr>")
        file.write("</table>")
        
        file.write("<h1>Sync Result</h1>")
        file.write("<table><tr><th>Stream</th><th>Replication Method</th><th>Sync 1 Record counts</th><th>Sync 1</th><th>Sync state Record counts</th><th>Sync with bookmark</th></tr>")
        for y in streams[branches]:
            file.write("\n<tr><td>{stream}</td><td>{method}</td><td>{records_1}</td><td>{sync_1}</td><td>{records_s}</td><td>{sync_s}</td></tr>".format(stream=y, method=ReplicationMethod[branches][y], records_1=sync1[branches][y], sync_1=bookmarks[branches][y], records_s=syncs[branches][y], sync_s=bookmarks_updated[branches][y]))
        file.write("</table>")
        file.write("<h1>Extra BookMarks</h1>")
        file.write("<table><tr><th>Stream</th><th>State 1</th><th>State with bookmark</th></tr>")
        for y in list(bookmarks[branches].keys()):
            if y not in streams[branches]:
                file.write("\n<tr><td>{stream}</td><td>{sync_1}</td><td>{sync_s}</td></tr>".format(stream=y, sync_1=bookmarks[branches][y], sync_s=bookmarks_updated[branches][y]))
        file.write("</table>")
        file.write("<h1>MAX REPLICATION VALUE IN RECORDS</h1>")
        file.write("<table><tr><th>Stream</th><th>MAX VALUE</th><th>MAX VALUE with state</th></tr>")
        for y in streams[branches]:
            if ReplicationMethod[branches][y] == "INCREMENTAL":
                file.write("\n<tr><td>{stream}</td><td> {sync_1} </td><td> {sync_s} </td></tr>".format(stream=y, sync_1=max_bookmark_value[branches][y], sync_s=max_bookmark_value_state[branches][y]))
        file.write("</table>")
        file.write("<h1>Unique RECORDS count</h1>")
        file.write("<table><tr><th>Stream</th><th>Unique RECORD count [List, Set]: Sync 1</th></tr>")
        for y in streams[branches]:
            file.write("\n<tr><td>{stream}</td><td>{sync_1}</td></tr>".format(stream=y, sync_1=unique_sync1[branches][y]))
        file.write("</table>")
        file.write(end_text)
        file.close()
