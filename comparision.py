from collections import defaultdict
import json
import os
import sys


def catalog_compare(input_arr, newly_added_fields, removed_fields, available_fields, automatic_fields, unsupported_fields, streams, added_uncommon_pairs, removed_uncommon_pairs, final_fields, final_fields_format, added_format, removed_format):

    key = list(input_arr.keys())
    folder_name = input_arr[key[0]]['folder_name']
    path = input_arr[key[0]]['path']
    tap_name = input_arr[key[0]]['tap_name']

    all_fields = defaultdict(dict)
    for keys in key: 
        for stream in streams[keys]:
            all_fields[keys][stream] = list((set(available_fields[keys][stream]).union(set(automatic_fields[keys][stream]))).union(set(unsupported_fields[keys][stream])))
            if '-' in all_fields[keys][stream]:
                all_fields[keys][stream] = list(set(all_fields[keys][stream]) - set('-'))

    diff_streams = []
    diff_streams_1 = []

    if streams[key[0]] == streams[key[1]]:
        for stream in streams[key[1]]:
            newly_added_fields[stream] = list(set(all_fields[key[0]][stream]) - set(all_fields[key[1]][stream]))
            removed_fields[stream] = list(set(all_fields[key[1]][stream]) - set(all_fields[key[0]][stream]))

    else:
            diff_streams_1 = list(set(streams[key[0]]) - set(streams[key[1]]))
            temp_streams_3 = list(set(streams[key[0]]) - set(diff_streams_1))
            diff_streams = temp_streams_3
            if len(diff_streams) != 0:
                for stream in diff_streams:
                    newly_added_fields[stream] = list(set(all_fields[key[0]][stream]) - set(all_fields[key[1]][stream]))
                    removed_fields[stream] = list(set(all_fields[key[1]][stream]) - set(all_fields[key[0]][stream]))
            if len(diff_streams_1) != 0:
                for stream in diff_streams_1:
                    newly_added_fields[stream] = list(all_fields[key[0]][stream])

            diff_streams_1 = []
            diff_streams_1 = list(set(streams[key[1]]) - set(streams[key[0]]))
            if len(diff_streams_1) != 0:
                for stream in diff_streams_1:
                    removed_fields[stream] = list(all_fields[key[1]][stream])
        
    def loader(branches, fields, stream, field2):
        for field, data in fields.items():
            if field2:
                appending_field = field2 + '.' + field
            else:
                appending_field = field
            if 'properties' in data:
                if data['properties'] != {}:
                    final_fields[branches][stream][appending_field] = data['type']
                    loader(branches, data['properties'], stream, appending_field)
                else:
                    final_fields[branches][stream][appending_field] = data['type']
                    
                    if 'format' in data:
                        final_fields_format[branches][stream][appending_field] = data['format']
                    
            elif "items" in data and "properties" in data["items"]:
                
                final_fields[branches][stream][appending_field] = data['type']
                appending_field = appending_field+".items"
                final_fields[branches][stream][appending_field] = data['items']['type']
                loader(branches, data["items"]["properties"], stream, appending_field)
            
            elif 'items' in data and "properties" not in data["items"]:
                
                final_fields[branches][stream][appending_field] = data['type']
                
                if data['items'] != {}:
                    
                    appending_field = appending_field + '.items'
                    final_fields[branches][stream][appending_field] = data['items']['type']
                    
                    if 'format' in data['items']:
                        final_fields_format[branches][stream][appending_field] = data['items']['format']

                if data['items'] == {}:
                    
                    appending_field = appending_field + '.items'
                    final_fields[branches][stream][appending_field] = list('{}')
                    
                    if 'format' in data['items']:
                        final_fields_format[branches][stream][appending_field] = data['items']['format']

            elif 'anyOf' in data:
                final_fields[branches][stream][appending_field] = []
                for x in range(len(data['anyOf'])):    
                    final_fields[branches][stream][appending_field].append(data['anyOf'][x]['type'])
                    
                for x in range(len(data['anyOf'])):
                    
                    if "items" in data['anyOf'][x] and "properties" in data['anyOf'][x]["items"]:
                        
                        appending_field = appending_field+".items"
                        final_fields[branches][stream][appending_field] = data['anyOf'][x]['items']['type']
                        loader(branches, data['anyOf'][x]['items']['properties'], stream, appending_field)
                        
                    elif 'properties' in data['anyOf'][x]:
                        if data['anyOf'][x]['properties'] != {}:
                            
                            final_fields[branches][stream][appending_field] = data['anyOf'][x]['type']
                            loader(branches, data['anyOf'][x]['properties'], stream, appending_field)
                        
                        else:

                            final_fields[branches][stream][appending_field] = data['anyOf'][x]['type']
                            
                            if 'format' in data['anyOf'][x]:
                                final_fields_format[branches][stream][appending_field] = data['anyOf'][x]['format']
                            
            else:
                if 'type' in data:
                    final_fields[branches][stream][appending_field] = data['type']
                    
                    if 'format' in data:
                        final_fields_format[branches][stream][appending_field] = data['format']
                else:
                    final_fields[branches][stream][appending_field] = data
                
    for branches in input_arr:
            folder_name = input_arr[branches]['folder_name']
            path = input_arr[branches]['path']
            tap_name = input_arr[branches]['tap_name']
            with open(path+'/'+folder_name+'/tap-'+tap_name+'/catalog.json', 'r+') as f:
                json_load = json.load(f)
                data = json_load['streams']
                for x in range(int(len(data))):
                    schema = data[x]['schema']['properties']
                    final_fields[branches][data[x]["stream"]] = {}
                    final_fields_format[branches][data[x]["stream"]] = {}
                    loader(branches, schema, data[x]["stream"], "")
                f.close()

    
    diff_streams = []
    diff_streams_1 = []
    
    def schema_compare(diff_streams):

        for stream in diff_streams:
            if stream in final_fields[key[0]]:
                for field in final_fields[key[0]][stream]:
                    if (field in final_fields[key[1]][stream] and final_fields[key[0]][stream][field] == final_fields[key[1]][stream][field]):
                        pass
                    else:
                        added_uncommon_pairs[stream][field] = final_fields[key[0]][stream][field]

            if stream in final_fields_format[key[0]]:   
                for field in final_fields_format[key[0]][stream]:
                    if (field in final_fields_format[key[1]][stream] and final_fields_format[key[0]][stream][field] == final_fields_format[key[1]][stream][field]):
                        pass
                    else:
                        added_format[stream][field] = final_fields_format[key[0]][stream][field]

            if stream in final_fields[key[1]]:
                for field in final_fields[key[1]][stream]:
                    if (field in final_fields[key[0]][stream] and final_fields[key[1]][stream][field] == final_fields[key[0]][stream][field]):
                        pass
                    else:
                        removed_uncommon_pairs[stream][field] = final_fields[key[1]][stream][field]

            if stream in final_fields_format[key[1]]:
                for field in final_fields_format[key[1]][stream]:
                    if (field in final_fields_format[key[0]][stream] and final_fields_format[key[1]][stream][field] == final_fields_format[key[0]][stream][field]):
                        pass
                    else:
                        removed_format[stream][field] = final_fields_format[key[1]][stream][field]

    if list(final_fields[key[0]].keys()) == list(final_fields[key[1]].keys()):
        for stream in final_fields[key[0]]:
            is_equal = final_fields[key[0]][stream] == final_fields[key[1]][stream]
            if not is_equal:
                diff_streams.append(stream)

        if len(diff_streams) != 0:
            schema_compare(diff_streams)

    else:
            temp_streams_1 = list(final_fields[key[0]].keys())
            temp_streams_2 = list(final_fields[key[1]].keys())
            diff_streams_1 = list(set(temp_streams_1) - set(temp_streams_2))
            temp_streams_3 = list(set(temp_streams_1) - set(diff_streams_1))
            diff_streams = temp_streams_3
            if len(diff_streams) != 0:
                schema_compare(diff_streams)
            if len(diff_streams_1) != 0:
                for stream in diff_streams_1:
                    if stream in final_fields[key[0]]:
                        for field in final_fields[key[0]][stream]:
                            added_uncommon_pairs[stream][field] = final_fields[key[0]][stream][field]
                    if stream in final_fields_format[key[0]]:
                        for field in final_fields_format[key[0]][stream]:
                            added_format[stream][field] = final_fields_format[key[0]][stream][field]
        
            diff_streams_1 = []
            diff_streams_1 = list(set(temp_streams_2) - set(temp_streams_1))
            if len(diff_streams_1) != 0:
                for stream in diff_streams_1:
                    if stream in final_fields[key[1]]:
                        for field in final_fields[key[1]][stream]:
                            removed_uncommon_pairs[stream][field] = final_fields[key[1]][stream][field]
                    if stream in final_fields_format[key[1]]:
                        for field in final_fields_format[key[1]][stream]:
                            removed_format[stream][field] = final_fields_format[key[1]][stream][field]

    return newly_added_fields, removed_fields, added_uncommon_pairs, removed_uncommon_pairs, final_fields_format, added_format, removed_format

def genrate_comparision_report(input_arr, main_file_path, streams, added_uncommon_pairs, removed_uncommon_pairs, newly_added_fields, removed_fields, updated_uncommon_pairs, ReplicationMethod, ReplicationKeys, automatic_fields, unsupported_fields, available_fields, added_format, removed_format):
    
    top = '''
    <!DOCTYPE html>
    <html>
    <style>
    th, td {
    border:1px solid black;
    border-style: dotted;
    vertical-align: top;
    }
    ul {
    display: inline-block;
    text-align: left;
    }
    div {
    border:2px solid black;
    }
    </style>
    <body>
    '''

    end_text = '''
    </body>
    </html>
    '''
        
    tap_names = []
    branch_names = []
    for branch in input_arr:
            tap_names.append(input_arr[branch]['tap_name'])
            branch_names.append(branch)
    
    updated_format = defaultdict(dict)
    if tap_names[0] == tap_names[1]:
        
            tap_name = input_arr[branch_names[0]]['tap_name']

            cmd_output = os.system('rm -rf '+main_file_path+'/'+tap_name+'_Comparision.html')
            if cmd_output != 0:
                sys.exit()
            file = open(main_file_path+"/"+tap_name+"_Comparision.html","a")
            file.write(top)
            
            body = '''
            <table style="width:100%; text-align: center"><td style='border:3px solid black; border-style: dashed;'><h1>Tap-{name}</h1>
            <h2>{branch}</h2></td></table>
            '''.format(name=tap_name, branch=branch_names[0]+' <---> '+branch_names[1])

            file.write(body)
            
            for y in list(set(streams[branch_names[0]]).union(set(streams[branch_names[1]]))):
                file.write("<h3><a href='#{href_stream}'> ♦ {stream}</a></h3>".format(href_stream=y, stream=y))

            for y in list(set(streams[branch_names[0]]).union(set(streams[branch_names[1]]))):

                    if (y in list(added_uncommon_pairs.keys())) and (y in list(removed_uncommon_pairs.keys())): 
                        for field in added_uncommon_pairs[y]:
                            if field in list(removed_uncommon_pairs[y].keys()):
                                if added_uncommon_pairs[y][field] != removed_uncommon_pairs[y][field]:
                                    updated_uncommon_pairs[y][field] = []
                                    updated_uncommon_pairs[y][field].append(removed_uncommon_pairs[y][field])
                                    updated_uncommon_pairs[y][field].append(added_uncommon_pairs[y][field])
                                    
                    for field in updated_uncommon_pairs[y]:  
                        added_uncommon_pairs[y].pop(field)
                        removed_uncommon_pairs[y].pop(field)

                    if (y in list(added_format.keys())) and (y in list(removed_format.keys())): 
                        for field in added_format[y]:
                            if field in list(removed_format[y].keys()):
                                if added_format[y][field] != removed_format[y][field]:
                                    updated_format[y][field] = []
                                    updated_format[y][field].append(removed_format[y][field])
                                    updated_format[y][field].append(added_format[y][field])
                                    
                    for field in updated_format[y]:  
                        added_format[y].pop(field)
                        removed_format[y].pop(field)                                    

                    file.write("<h1 id={href_stream}><u>{stream}</u></h1><div class='row'><table style='width:100%;'><tr><td style='border:1.5px solid black; border-style: dashed;'>".format(href_stream=y, stream=y))
                    if (y in list(available_fields[branch_names[0]].keys())) and (y in list(available_fields[branch_names[1]].keys())):
                        if  available_fields[branch_names[0]][y] != available_fields[branch_names[1]][y]:
                            file.write("<h3>Comparision of Inclusion Availbale Fields</h3>")
                            file.write("<table><tr><th style='border:1.5px solid black;'> {branch1} (Added) </th><th style='border:1.5px solid black;'> {branch2} (Removed) </th></tr>".format(branch1=branch_names[0], branch2=branch_names[1]))
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=list(set(available_fields[branch_names[0]][y]) - set(available_fields[branch_names[1]][y]))))
                            file.write("</td>")
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=list(set(available_fields[branch_names[1]][y]) - set(available_fields[branch_names[0]][y]))))
                            file.write("</td>")
                            file.write("</table>")    
                        else:
                            file.write("<h4>No diff found for Inclusion Availbale Fields</h4>")
                    file.write("</td></tr>")
                    file.write("<tr><td style='border:1.5px solid black; border-style: dashed;'>")
                    if (y in list(automatic_fields[branch_names[0]].keys())) and (y in list(automatic_fields[branch_names[1]].keys())):
                        if  automatic_fields[branch_names[0]][y] != automatic_fields[branch_names[1]][y]:
                            file.write("<h3>Comparision of Inclusion Automatic Fields</h3>")
                            file.write("<table><tr><th style='border:1.5px solid black;'> {branch1} (Added) </th><th style='border:1.5px solid black;'> {branch2} (Removed) </th></tr>".format(branch1=branch_names[0],   
                            branch2=branch_names[1]))
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=list(set(automatic_fields[branch_names[0]][y]) - set(automatic_fields[branch_names[1]][y]))))
                            file.write("</td>")
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=list(set(automatic_fields[branch_names[1]][y]) - set(automatic_fields[branch_names[0]][y]))))
                            file.write("</td>")
                            file.write("</table>")    
                        else:
                            file.write("<h4>No diff found for Inclusion Automatic Fields</h4>")
                    file.write("</td></tr>")
                    file.write("<tr><td style='border:1.5px solid black; border-style: dashed;'>")
                    if (y in list(unsupported_fields[branch_names[0]].keys())) and (y in list(unsupported_fields[branch_names[1]].keys())):
                        if  unsupported_fields[branch_names[0]][y] != unsupported_fields[branch_names[1]][y]:
                            file.write("<h3>Comparision of Inclusion Unsupported Fields</h3>")
                            file.write("<table><tr><th style='border:1.5px solid black;'> {branch1}(Added) </th><th style='border:1.5px solid black;'> {branch2} (Removed) </th></tr>".format(branch1=branch_names[0], branch2=branch_names[1]))
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=list(set(unsupported_fields[branch_names[0]][y]) - set(unsupported_fields[branch_names[1]][y]))))
                            file.write("</td>")
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=list(set(unsupported_fields[branch_names[1]][y]) - set(unsupported_fields[branch_names[0]][y]))))
                            file.write("</td>")
                            file.write("</table>")    
                        else:
                            file.write("<h4>No diff found for Inclusion Unsupported Fields</h4>")
                    file.write("</td></tr>")
                    file.write("<tr><td style='border:1.5px solid black; border-style: dashed;'>")
                    if (y in list(ReplicationMethod[branch_names[0]].keys())) and (y in list(ReplicationMethod[branch_names[1]].keys())):
                        if  ReplicationMethod[branch_names[0]][y] != ReplicationMethod[branch_names[1]][y]:
                            file.write("<h3>Comparision of Replication Method</h3>")
                            file.write("<table><tr><th style='border:1.5px solid black;'> {branch1} </th><th style='border:1.5px solid black;'> {branch2} </th></tr>".format(branch1=branch_names[0], branch2=branch_names[1]))
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=ReplicationMethod[branch_names[0]][y]))
                            file.write("</td>")
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=ReplicationMethod[branch_names[1]][y]))
                            file.write("</td>")
                            file.write("</table>")    
                        else:
                            file.write("<h4>No diff found in Replication Method</h4>")
                    file.write("</td></tr>")
                    file.write("<tr><td style='border:1.5px solid black; border-style: dashed;'>")
                    if (y in list(ReplicationKeys[branch_names[0]].keys())) and (y in list(ReplicationKeys[branch_names[1]].keys())):
                        if  ReplicationKeys[branch_names[0]][y] != ReplicationKeys[branch_names[1]][y]:
                            file.write("<h3>Comparision of Replication Keys</h3>")
                            file.write("<table><tr><th style='border:1.5px solid black;'> {branch1} </th><th style='border:1.5px solid black;'> {branch2} </th></tr>".format(branch1=branch_names[0], branch2=branch_names[1]))
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=ReplicationKeys[branch_names[0]][y]))
                            file.write("</td>")
                            file.write("<td style='border:1.5px solid black;'>")
                            file.write("<li>{added}</li>".format(added=ReplicationKeys[branch_names[1]][y]))
                            file.write("</td>")
                            file.write("</table>")    
                        else:
                            file.write("<h4>No diff found in Replication Keys</h4>")
                    file.write("</td></tr>")
                    file.write("<tr><td style='border:1.5px solid black; border-style: dashed;'>")
                    if (y in list(newly_added_fields.keys())) or (y in list(removed_fields.keys())):
                        if len(newly_added_fields[y]) != 0 or len(removed_fields[y]) != 0:
                            file.write("<h3>Comparision in Metadata</h3>")
                            file.write("<table><tr><th style='border:1.5px solid black;'> {branch1} (Added) </th><th style='border:1.5px solid black;'> {branch2} (Removed) </th></tr>".format(branch1=branch_names[0], branch2=branch_names[1]))
                            file.write("<td style='border:1.5px solid black;'>")
                            if y in list(newly_added_fields.keys()):
                                file.write("<ul>")
                                # newly_added_fields[y].sort()    
                                for field in newly_added_fields[y]:
                                    file.write("<li>{added}</li>".format(added=field))
                                file.write("</ul>")
                            file.write("</td>")
                            file.write("<td style='border:1.5px solid black;'>")
                            if y in list(removed_fields.keys()):
                                file.write("<ul>")
                                # removed_fields[y].sort()
                                for field in removed_fields[y]:
                                    file.write("<li>{added}</li>".format(added=field))
                                file.write("</ul>")
                            file.write("</td>")
                            file.write("</table>")    
                        else:
                            file.write("<h4>No diff found in Metadata</h4>")
                    file.write("</td></tr>")    
                    file.write("<tr><td style='border:1.5px solid black; border-style: dashed;'>")
                    if y in list(added_uncommon_pairs.keys()) or y in list(removed_uncommon_pairs.keys()) or y in list(updated_uncommon_pairs.keys()) or y in list(added_format.keys()) or y in list(removed_format.keys()) or y in list(updated_format.keys()):
                        if len(added_uncommon_pairs[y]) != 0 or len(removed_uncommon_pairs[y]) != 0 or len(updated_uncommon_pairs[y]) != 0 or len(added_format[y]) != 0 or len(removed_format[y]) != 0 or len(updated_format[y]) != 0:
                            file.write("<h3>Comparision in Schemas</h3>")
                            file.write("<table><tr><th style='border:1.5px solid black;'> {branch1} (Added) </th><th style='border:1.5px solid black;'> {branch2} (Removed) </th><th style='border:1.5px solid black;'> Updated Schema </th></tr>".format(branch1=branch_names[0], branch2=branch_names[1]))
                            file.write("<td style='border:1.5px solid black;'>")
                            if y in list(added_uncommon_pairs.keys()) or y in list(added_format.keys()):
                                file.write("<table>")
                                if y not in list(added_uncommon_pairs.keys()):
                                    added_uncommon_pairs[y]['-'] = '-'
                                if y not in list(added_format.keys()):
                                    added_format[y]['-'] = '-'
                                temp_count = 0
                                for field in list(set(added_uncommon_pairs[y].keys()).union(set(added_format[y].keys()))):
                                    if temp_count == 0:
                                        file.write("<tr><td><p><b>Field</b></p></td><td style='border:1.5px solid black;'><b>-</b></td><td><p><b>Data Type</b></p></td><td><p><b>Format</b></p></td></tr>")
                                    if field == "-":
                                        pass
                                    else:
                                        if field not in added_format[y]:
                                            formats_ref = '-'
                                        else:
                                            formats_ref=added_format[y][field]
                                        if field not in added_uncommon_pairs[y]:
                                            field_name_ref = 'No  diff'
                                        else:
                                            field_name_ref = added_uncommon_pairs[y][field]
                                        name_field = field.replace('.', ' -> ')
                                        file.write("<tr><td style='text-align:left'>{added}</td><td style='border:1.5px solid black;'><b>-</b></td><td>{field_name}</td><td>{formats}</td></tr>".format(added=name_field, field_name=field_name_ref, formats=formats_ref))
                                    temp_count += 1
                                file.write("</table>")
                            file.write("</td>")
                            file.write("<td style='border:1.5px solid black;'>")
                            if y in list(removed_uncommon_pairs.keys()) or y in list(removed_format.keys()):
                                file.write("<table>")
                                if y not in list(removed_uncommon_pairs.keys()):
                                    removed_uncommon_pairs[y]['-'] = '-'
                                if y not in list(removed_format.keys()):
                                    removed_format[y]['-'] = '-'
                                temp_count = 0
                                for field in list(set(removed_uncommon_pairs[y].keys()).union(set(removed_format[y].keys()))):
                                    if temp_count == 0:
                                        file.write("<tr><td><p><b>Field</b></p></td><td style='border:1.5px solid black;'><b>-</b></td><td><p><b>Data Type</b></p></td><td><p><b>Format</b></p></td></tr>")
                                    if field == "-":
                                        pass
                                    else:
                                        if field not in removed_format[y]:
                                            formats_ref = '-'
                                        else:
                                            formats_ref=removed_format[y][field]
                                        if field not in removed_uncommon_pairs[y]:
                                            field_name_ref = 'No  diff'
                                        else:
                                            field_name_ref = removed_uncommon_pairs[y][field]
                                        name_field = field.replace('.', ' -> ')
                                        file.write("<tr><td style='text-align:left'>{added}</td><td style='border:1.5px solid black;'><b>-</b></td><td>{field_name}</td><td>{formats}</td></tr>".format(added=name_field, field_name=field_name_ref, formats=formats_ref))
                                    temp_count += 1
                                file.write("</table>")
                            file.write("</td>")
                            file.write("<td style='border:1.5px solid black;'>")
                            if y in list(updated_uncommon_pairs.keys()) or y in list(updated_format.keys()):
                                file.write("<table>")
                                if y not in list(updated_uncommon_pairs.keys()):
                                    updated_uncommon_pairs[y]['-'] = '-'
                                if y not in list(updated_format.keys()):
                                    updated_format[y]['-'] = '-'
                                temp_count = 0
                                for field in list(set(updated_uncommon_pairs[y].keys()).union(set(updated_format[y].keys()))):
                                    if temp_count == 0:
                                        file.write("<tr><td><p><b>Field</b></p></td><td style='border:1.5px solid black;'><b>-</b></td><td><p><b>Data Type</b></p></td><td><p><b>Format</b></p></td></tr>")
                                    if field == "-":
                                        pass
                                    else:
                                        if field not in updated_format[y]:
                                            formats_ref = '-'
                                            formats_ref_1 = ''
                                        else:
                                            formats_ref=updated_format[y][field][0]
                                            formats_ref_1=updated_format[y][field][1]
                                        if field not in updated_uncommon_pairs[y]:
                                            field_name_ref = 'No  diff'
                                            field_name_ref_1 = ''
                                        else:
                                            field_name_ref = updated_uncommon_pairs[y][field][0]
                                            field_name_ref_1 = updated_uncommon_pairs[y][field][1]
                                        name_field = field.replace('.', ' -> ')
                                        file.write("<tr><td style='text-align:left'>{updated}</td><td style='border:1.5px solid black;'><b>-</b></td><td>{field_name} -> {field_name_1}</td><td>{formats} -> {formats_1}</td></tr>".format(updated=name_field, field_name=field_name_ref, field_name_1=field_name_ref_1, formats=formats_ref, formats_1=formats_ref_1))
                                    temp_count += 1
                                file.write("</table>")
                            file.write("</td>")
                            file.write("</table>")
                        else:
                            file.write("<h4>No diff found in Schemas</h4>")
                    file.write("</td></tr></table></div>")
            file.write(end_text)
            file.close()
