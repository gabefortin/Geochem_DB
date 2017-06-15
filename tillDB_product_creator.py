# -*- coding: cp1252 -*-
'''+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  This script is coded to generate 'data_sheet.csv', a flat view data product derived from the TillDB.
  This product is preferred for conducting geochem data QA/QC and analysis.

  Input
         1) TillDB path
         2) Output file path
         3) BCGS mapsheet path
           
  Output     
         "data_sheet.csv" with the following attributes (sample_id, sample_name, pub_issue, ...,
         nad83_lat, nad83_long, coord_conf, nts_mapsheet, ..., analyte_method_unit_size, ...)
        
  Additonal info
         1) The structure of "data_sheet.csv" is the similar to the one derived from the lithoDB.
         
         2) Sub-routine "get_ntssheet" is for NTS 50k map grid tag retrieval based on the given sample
            location and NTS map grid shape file. Both of them are assumed using the same
            spatial reference system: NAD83 geographic (EPSG code = 4269).
            
         3) Pyproj replaces OGR/OSR for coordinate re-projecting, because the later does not address
            datum shift during re-projection. 

         4) "data_sheet" is created by this script through a process of generalization . Each analyte
            in the 'data_analyte' table of the database is described by multiple attributes, include,
            analyte name, analysis result, size fraction, analysis method, unit, minimum detection unit,
            lab, sample_id etc. In 'data_sheet.csv' created by this script, however, the majority columns
            are in the format of 'analyte-name_mothod_unit_size'. So only 4 attributes above for each
            analyte are used. This means that sample analyte results can be put under the same column
            as long as their analyte name, method, unit, and size fraction are the same (even if these
            samples are analyzed by different labs with the different minimum detection limit).
            
         5) Since samples may be re-analyzed across different years,  columns with the same header
            (analyte_method_unit_size), such as Rb_INA_ppm_63 for example, can happen. The maximum
            number of repeat is 9. This threshold number is decided by experience from the LithoDB,
            where the same column header was found repeated 5 times.
            
         6) It has been noted that some analyte values were rounded and published differently.
            For example, in publication PA, sample SA's Au value was reported as 10.2 ppb analyzed using
            method MA. In publication PB, however, the same sample's Au was rounded and reported as 10 ppb
            (analyzed by the same method). This caused 2 columns: "Au_ppb_MA" and "Au1_ppb_MA" being
            created in the data product file. So these all columns of this type in the data product file
            should be examined mannually.
          
  Status
         Operational
         
  Last update
         2017-03-17

  Future improvement
         1) The hardcoded maximum number of repeated headers (mentioned above) should be removed.
  +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'''
import os, sys, csv, pyodbc, re, ogr, osr
from pyproj import Proj, transform
#========================================== Sub-routines =======================================
#Get an attribute value from a given table based on a given key attribute
#Syntax: get_name(db_cursor, string, string, string, int) return string
def get_name(db_cur, fld_name, tab_name, key_name, key_val):
    db_cur.execute('select %s from %s where %s = %s' %(fld_name, tab_name, key_name, key_val))
    record = db_cur.fetchone()
    if record == None:
        print 'No such ' + key_name + ' exists in ' + tab_name + ' table!'
        sys.exit()
    else:
        return record[0]

#Project input coordinates to NAD83 long. and lat.
#Syntax: project2nad83(float, float, int) returns [float, float]
def project2nad83 (source_x, source_y, source_epsg):

    if source_epsg == 4269: #Already in NAD83 and no projection needed
        return [source_x, source_y]
    
    else:
        source_epsg = "+init=EPSG:" + str(source_epsg)
        source_srf = Proj(source_epsg)
        target_srf = Proj("+init=EPSG:4269")

        target_x, target_y = transform(source_srf, target_srf, source_x, source_y)
        
        return [target_x, target_y]

#Project a pair of NAD83 long. and lat. to NAD83_UTM coordinates
#Syntax: project2utm(float, float) returns [int, int, int]
def project2utm (nad83_long, nad83_lat):

    #Determine UTM zone
    utm_zone = 0
    epsg_id = 0
    if (nad83_long >= -144) and (nad83_long <= -138):
        utm_zone = 7
        epsg_id = 26907
    elif (nad83_long >= -138) and (nad83_long <= -132):
        utm_zone = 8
        epsg_id = 26908
    elif (nad83_long >= -132) and (nad83_long <= -126):
        utm_zone = 9
        epsg_id = 26909
    elif (nad83_long >= -126) and (nad83_long <= -120):
        utm_zone = 10
        epsg_id = 26910
    elif (nad83_long >= -120) and (nad83_long <= -114):
        utm_zone = 11
        epsg_id = 26911
    
    if (utm_zone == 0) or (epsg_id == 0):
        return [-1, -1, -1]
    else:
        source_srf = Proj("+init=EPSG:4269")
        target_epsg = "+init=EPSG:" + str(epsg_id)
        target_srf = Proj(target_epsg)

        target_x, target_y = transform(source_srf, target_srf, nad83_long, nad83_lat)

        return [int(target_x), int(target_y), utm_zone]
    
#Get NTS 50k mapsheet based on the given lat and long
#Syntax: get_ntssheet(str, float, float) return str
def get_ntssheet(mapsheet_file, nad83_long, nad83_lat):    
    driver = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(mapsheet_file, 0)
    layer = dataSource.GetLayer()
    for feature in layer:
        geom = feature.GetGeometryRef()
        bbox = geom.GetEnvelope()
        sheet_tag = feature.GetField("map_tile")
        if ((nad83_long >= bbox[0]) and (nad83_long < bbox[1]) and \
            (nad83_lat >= bbox[2]) and (nad83_lat < bbox[3])):
            return sheet_tag
    return ' ' #indicating boundary fall (out of provincial boudary)
#========================================================================================================
def main():
    #Input info
    db_path = 'C:\\Project\\TillDB\data\\tillDB_curr.accdb'
    data_sheet = 'C:\\Project\\TillDB\\data\\data_sheet.csv'
    nts_mapsheet = 'C:\\Project\\ProvinceData\\topo_data\\nts_50k\\grid_50k_nts_ll83_poly.shp'
    
    #Database connection
    db_conn = pyodbc.connect('Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+db_path)
    cur = db_conn.cursor()

    csv_output = open(data_sheet, 'wb')
    csv_writer = csv.writer(csv_output, delimiter = ',')
    
    #Get all "sample_id"s from 'data_sample' table
    sample_list = []
    cur.execute("""select sample_id from data_sample order by sample_id""")
    all_records = cur.fetchall()
    for record in all_records:
        sample_list.append(record[0])
    #+++++++++++++++++++++++++++++++++ Construct "data_sheet.csv" header ++++++++++++++++++++++++++
    print 'Creating header row ...'
    #Working variale to store the maximum number of publications related to a single sample
    max_issue = 0

    #Working variables to store analyte, method_id, unit_id, and other header items
    file_header = []
    sample_queue = []
        
    #Loop through the sample_ids collected in "sample_list"
    for sample in sample_list:
        cur.execute("""select analyte, size_frac, method_id, unit_id from data_analyte
                              where sample_id = ? order by analyte""", sample) 
        all_records = cur.fetchall()
        
        for record in all_records:
            analyte = record[0].replace(' ', '')

            size_frac = str(record[1])
            
            method_id = record[2]
            method_group = get_name(cur, 'method_group', 'code_method', 'method_id', method_id)

            unit_id = record[3]
            unit_name = get_name(cur, 'name', 'code_unit', 'unit_id', unit_id)

            item = analyte + '_' + method_group + '_' + unit_name + '_' + size_frac

            #Check existence and update "file_header"
            #"9" below is set by experience, which indicates the maximum number allowed
            #for repeated column names of the same "analyte_method_unit_size".
            occr = file_header.count(item)   
            if (sample not in sample_queue) and (occr == 0):
                file_header.append(item)
                sample_queue.append(sample)
            elif (sample not in sample_queue) and (occr > 0) and (occr < 9):
                sample_queue.append(sample)
            elif (sample in sample_queue) and (occr == 0):
                file_header.append(item)
            elif (sample in sample_queue) and (occr > 0) and (occr < 9):
                file_header.append(item)
         
        #Deal with pub_issue (one sample can be published in multiple reports)
        cur.execute("""select count(*) from data_publish where sample_id = ?""", sample)
        issue_count = cur.fetchone()
        if int(issue_count[0]) > max_issue:
            max_issue = int(issue_count[0])
    
    #Add the static/fixed items to the "file_header"
    file_header.insert(0, 'Sample_ID')
    file_header.insert(1, 'Sample_Code')
    file_header.insert(2, 'Sample_Name')
    file_header.insert(3, 'Sample_Type')
    file_header.insert(4, 'Depth')
    file_header.insert(5, 'Deplicate')
    file_header.insert(6, 'Borehole')
    file_header.insert(7, 'Core_Top')
    file_header.insert(8, 'Core_Bottom')                   
    file_header.insert(9, 'Azimuth')
    file_header.insert(10, 'Dip')
    file_header.insert(11, 'Drill_Type')
    file_header.insert(12, 'Material_Type')
    file_header.insert(13, 'Sample_Desp')
    file_header.insert(14, 'NAD83_Long')
    file_header.insert(15, 'NAD83_Lat')
    file_header.insert(16, 'Elev')
    file_header.insert(17, 'Coord_Conf')
    file_header.insert(18, 'NTS_Map')
    file_header.insert(19, 'UTM_Easting')
    file_header.insert(20, 'UTM_Northing')
    file_header.insert(21, 'UTM_Zone')

    #Add pub_issues (dynamic) to the file_header
    for issue in range(0, max_issue):
        issue_header = 'Pub_Issue'
        if issue > 0:
            issue_header = 'Pub_Issue' + str(issue)
        file_header.append(issue_header)
                 
    #Output header row
    csv_writer.writerow(file_header)
    #+++++++++++++++++++++++++++++++++++++++++ Create file content ++++++++++++++++++++++++++++++++++++++++
    print 'Constructing data rows ...'
    for sample in sample_list:
        #Create a list to store all values to be writen as a row to the output csv file
        data_row = ['']*len(file_header)

        #Retrieve and populate the static fields in "data_row"
        cur.execute("""select sample_id, sample_code, sample_name, sample_type, depth, duplicate, borehole,
                       core_top, core_bottom, azimuth, dip, drill_type, material_type, sample_desp,
                       x_coord, y_coord, z_coord, coord_conf, EPSG_SRID 
                       from data_sample where sample_id = ?""", sample)
        sample_row = cur.fetchone()
        
        for i in range(len(sample_row)):
            data_row[i] = str(sample_row[i]).replace('None', '') #All missing values are treated as blank

        #Round elevation to integer
        if data_row[16] <> '':
            data_row[16] = str(int(round(float(data_row[16]))))

        #Project to NAD83 geographic if needed
        [data_row[14], data_row[15]] = project2nad83 (float(sample_row[14]), float(sample_row[15]), int(sample_row[18]))

        #Project to NAD83 UTM if needed
        [data_row[19], data_row[20], data_row[21]] = project2utm(data_row[14], data_row[15])

        #Get NTS mapsheet
        data_row[18] = get_ntssheet(nts_mapsheet, data_row[14], data_row[15])
        
        #Retrieve and populate "analyte_method_unit_size" items (dynamic) in "data_row"         
        cur.execute("""select analyte, abundance, size_frac, method_id, unit_id from data_analyte
                              where sample_id = ?""", sample)
        all_records = cur.fetchall()
        for record in all_records:
            analyte = record[0]
            abundance = record[1].replace('None', '')   #All missing values are treated as blank
            
            size_frac = record[2]
            
            method_id = record[3]
            method_group = get_name(cur, 'method_group', 'code_method', 'method_id', method_id)

            unit_id = record[4]
            unit_name = get_name(cur, 'name', 'code_unit', 'unit_id', unit_id)

            item = analyte + '_' + method_group + '_' + unit_name + '_' + size_frac
            
            #Determine the right position (column) where current analyte abundance to fill in "data_row"
            item_occr = file_header.count(item)
            if item_occr == 0:   #No column matches (for debugging only)
                print 'Error! Header: ' + item + ' does no not exists!'
            elif item_occr == 1: #One column matches
                item_indx = file_header.index(item)
                if data_row[item_indx] <> '':
                    print 'Error! Spot is taken: ' + str(item_indx) #For debugging only
                else:   
                    data_row[item_indx] = abundance
            elif item_occr > 1:  #Multiple columns match
                #Get all indices of the matched columns
                item_indx = [j for j, x in enumerate(file_header) if x == item]
                for k in range(len(item_indx)):
                    #Former logic
                    #if data_row[item_indx[k]] == '':   
                    #    data_row[item_indx[k]] = abundance
                    #    break

                    #Current logic
                    if (data_row[item_indx[k]] == '') or (data_row[item_indx[k]] == abundance):   
                        data_row[item_indx[k]] = abundance
                        break
                        
        #Populate pub_issues (dynamic) in "data_row"
        p_indx = file_header.index('Pub_Issue')
        cur.execute("""select pub_issue from data_publish where sample_id = ?""", sample)
        all_issues = cur.fetchall()
        for issue in all_issues:
            data_row.insert(p_indx, issue[0])
            p_indx = p_indx + 1
                        
        csv_writer.writerow(data_row)
          
    csv_output.close()
    db_conn.close()
    #+++++++++++++++++++++++++++++++++++++++++ Remove blank columns ++++++++++++++++++++++++++++++++++++++++++
    #The logic used above creates blank columns, which have to be removed
    print 'Removing blank columns ...'
    #Open 'data_sheet.csv'
    old_file = open(data_sheet, 'rU')
    old_rows = csv.reader(old_file)

    #Get empty column indices
    old_header = old_rows.next()
    col_indx = [0]*len(old_header)
    col_indx[0:22] = [1]*22
    for old_row in old_rows:
        for ci in range(22, len(old_header)):
            if old_row[ci] <> '':
                col_indx[ci] = 1

    #Create a temp csv file
    file_path = os.path.dirname(data_sheet)
    temp_sheet = file_path + '\\temp_sheet.csv'
    temp_file = open(temp_sheet, 'wb')
    temp_writer = csv.writer(temp_file, delimiter = ',')

    #Write non-empty columns from 'data_sheet.csv' to the temp file    
    old_file.seek(0)    #rewind 'data_sheet.csv'
    for old_row in old_rows:
        new_row = []
        for ci in range(len(old_header)):
            if col_indx[ci] == 1:
                new_row.append(old_row[ci])
        temp_writer.writerow(new_row)
    old_file.close()
    temp_file.close()
    
    os.remove(data_sheet)
    os.rename(temp_sheet, data_sheet)
    #++++++++++++ Update names of header columns which have same "analyte_method_unit_size" combo ++++++++++++++
    print 'Updating header names ...'
    old_file = open(data_sheet, 'rU')
    old_rows = csv.reader(old_file)
    old_header = old_rows.next()    #Get the header row of the old file
    new_header = ['']*len(old_header)

    new_header[0] = old_header[0]
    for i in range(1, len(old_header)):
        item_count = old_header[0:i].count(old_header[i])
        if item_count == 0:
            new_header[i] = old_header[i]
        else:
            curr_item = old_header[i]
            p1 = curr_item[0:curr_item.find('_')] + str(item_count)
            p2 = curr_item[curr_item.find('_'):]
            new_header[i] = p1 + p2

    temp_sheet = file_path + '\\temp_sheet.csv'
    new_file = open(temp_sheet, 'wb')
    new_writer = csv.writer(new_file, delimiter = ',')
    new_writer.writerow(new_header)              
                
    #Extract data from the old 'data_sheet.csv' and output them to 'temp_sheet.csv' row by row
    for old_row in old_rows:
        new_writer.writerow(old_row)
    
    new_file.close()
    old_file.close()

    #Clean up
    os.remove(data_sheet)
    os.rename(temp_sheet, data_sheet)      
    #+++++++++++++++++++++++++++++++++++++++++ Sort columns ++++++++++++++++++++++++++++++++++++++++++++++
    #Group columns by method_code and order columns by element name within each group
    print 'Re-arranging columns ...'
    #Open 'data_sheet.csv'
    old_file = open(data_sheet, 'rU')
    old_rows = csv.reader(old_file)

    #Get the header row of the old file
    old_header = old_rows.next()    
    
    #Extract the first 4 columns and assign them the new_header
    new_header = old_header[0:3]

    #Add the publication columns to the new_header
    pub_count = 0
    for pub_str in old_header:
        if ('Pub_Issue' in pub_str) and (pub_count == 0):
            new_header.append('Pub_Issue')
            pub_count = pub_count + 1
        elif ('Pub_Issue' in pub_str) and (pub_count <> 0):
            new_header.append('Pub_Issue' + str(pub_count))
            pub_count = pub_count + 1
    
    #Add the remaining static columns
    new_header.extend(old_header[3:22])

    #Extract 'analyte_method_unit_frac' part (dynamic) from the old_header
    part4sort = old_header[22:(len(old_header) - max_issue)]
    
    method_part = []    #Extract 'method' from part4sort list
    analyte_part = []   #Extract analyte name from part4sort list
    unit_part = []      #Extract unit name from part4sort list
    frac_part = []      #Extract size fraction from part4sort list
    for i in xrange(len(part4sort)):
        indx1 = part4sort[i].find('_')
        indx2 = part4sort[i][(indx1 + 1):].find('_') + indx1 + 1
        indx3 = part4sort[i].rfind('_')
                             
        analyte_part.append(str(part4sort[i])[0:indx1])
        method_part.append(str(part4sort[i])[(indx1+1):indx2])
        unit_part.append(str(part4sort[i])[(indx2+1):indx3])
        frac_part.append(str(part4sort[i])[(indx3+1):len(part4sort[i])])
        
    #Create a list of tuples sorted by "method_code" first and analyte name second 
    combo = sorted(zip(method_part, analyte_part, unit_part, frac_part)) 

    #Complete the new header
    for i in range(len(combo)):
        for j in xrange(len(part4sort)):
            if (combo[i][0] == method_part[j]) and (combo[i][1] == analyte_part[j]) and \
               (combo[i][2] == unit_part[j]) and (combo[i][3] == frac_part[j]):
                new_header.append(analyte_part[j] + '_' + method_part[j] + '_' + unit_part[j] + '_' + frac_part[j])
                break

    #Create a temp csv file and write out the new header row
    file_path = os.path.dirname(data_sheet)
    temp_sheet = file_path + '\\temp_sheet.csv'
    new_file = open(temp_sheet, 'wb')
    new_writer = csv.writer(new_file, delimiter = ',')
    new_writer.writerow(new_header)

    #Extract data from the old 'data_sheet.csv' and output them to 'temp_sheet.csv' row by row
    for old_row in old_rows:
        new_row = []
        for new_item in new_header:
            old_indx = old_header.index(new_item)
            new_row.append(old_row[old_indx])
        
        new_writer.writerow(new_row)

    new_file.close()
    old_file.close()
    
    #Clean up
    os.remove(data_sheet)
    os.rename(temp_sheet, data_sheet)
    
    print 'Job done!'
    
if __name__ == "__main__":
    main()












